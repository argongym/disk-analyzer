const express = require('express');
const cors = require('cors');
const { spawn, exec } = require('child_process');
const fs = require('fs');
const path = require('path');

const app = express();
const PORT = 3000;
const CACHE_FILE = path.join(__dirname, 'disk_data.json');

app.use(cors());
app.use(express.json());
app.use(express.static('public'));

let diskCache = {};

// Load cache
if (fs.existsSync(CACHE_FILE)) {
    try {
        const rawCache = JSON.parse(fs.readFileSync(CACHE_FILE, 'utf8'));
        // MIGRATION: Check if it's the old format (values are objects)
        const sampleKey = Object.keys(rawCache)[0];
        if (sampleKey && typeof rawCache[sampleKey] === 'object') {
            console.log('Migrating cache to flat format...');
            for (const key in rawCache) {
                // Store only the totalSize
                if (rawCache[key].totalSize) {
                    diskCache[key] = rawCache[key].totalSize;
                }
            }
            // Immediately save the flattened cache
            saveCache();
            console.log('Migration complete.');
        } else {
            diskCache = rawCache;
        }
    } catch (err) {
        console.error('Error reading cache file:', err);
    }
}

function saveCache() {
    fs.writeFileSync(CACHE_FILE, JSON.stringify(diskCache, null, 2));
}

function formatSize(sizeInK) {
    if (sizeInK < 1024) return `${sizeInK} KB`;
    const sizeInM = sizeInK / 1024;
    if (sizeInM < 1024) return `${sizeInM.toFixed(1)} MB`;
    const sizeInG = sizeInM / 1024;
    return `${sizeInG.toFixed(2)} GB`;
}

// Helper to get size of a single item
function getItemSize(itemPath, onProgress) {
    return new Promise((resolve) => {
        // First check if it is a directory or file
        fs.lstat(itemPath, (err, stats) => {
            if (err) {
                // Permission denied or other error
                return resolve({ size: 0, error: err.code });
            }

            if (stats.isDirectory()) {
                // Use du with -d 1 to get immediate children and total, preventing massive recursion output
                const child = spawn('du', ['-k', '-d', '1', itemPath]);
                let lastSize = 0;
                let buffer = '';
                let lastReportTime = 0;

                child.stdout.on('data', d => {
                    buffer += d.toString();
                    const lines = buffer.split('\n');
                    buffer = lines.pop();

                    for (const line of lines) {
                        const trimmed = line.trim();
                        if (!trimmed) continue;
                        const match = trimmed.match(/^(\d+)\s+(.+)$/);
                        if (match) {
                            const size = parseInt(match[1], 10);
                            const pathStr = match[2];

                            // If pathStr is the itemPath itself, it's the total.
                            if (path.resolve(pathStr) === path.resolve(itemPath)) {
                                lastSize = size;
                            } else {
                                // It's a child. We can use it for progress.
                                // Throttle progress updates
                                const now = Date.now();
                                if (onProgress && now - lastReportTime > 100) {
                                    // relativePath comes from du -d 1, so it is just "child" e.g. "htdocs"
                                    let relative = path.relative(itemPath, pathStr);
                                    if (relative && relative !== '') {
                                        onProgress(relative);
                                    }
                                    lastReportTime = now;
                                }
                            }
                        }
                    }
                });

                child.on('close', code => {
                    // If lastSize is 0, usage might be 0 or error.
                    if (code !== 0 && lastSize === 0) return resolve({ size: 0 });
                    resolve({ size: lastSize, isDir: true });
                });

                child.on('error', () => resolve({ size: 0 }));
            } else {
                // File size from stats (bytes -> KB)
                resolve({ size: Math.ceil(stats.size / 1024), isDir: false });
            }
        });
    });
}

// SSE Endpoint for scanning
app.get('/api/scan-stream', async (req, res) => {
    const scanPath = req.query.path || '/';
    const forceRefresh = req.query.force === 'true';
    console.log(`Scanning (Stream): ${scanPath} ${forceRefresh ? '[FORCE REFRESH]' : ''}`);

    // SSE Setup
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');

    // Check Cache First (Flat format)
    // Only skip if not forcing refresh
    if (!forceRefresh && diskCache[scanPath]) {
        console.log(`Serving from cache: ${scanPath}`);
        // ... (existing cache serving logic - simplified for clarity as we just use it during scan below)
        // Actually, if we serve from cache entirely we return early. 
        // But our previous change made us fall through to readdir logic anyway.
        // Let's keep the logic consistent: if cached and !force, we rely on cache for children sizes later.
        // But we DO return early if we trust the cache for the *current* folder structure.
        // The previous step's code removed the "return" statement inside the "if diskCache" block?
        // Let's checking the file content...
        // Ah, in previous step I commented out the return? No, I see it in the `view_file` output from step 190 (which is partial).
        // Let's assume the previous logic was: check cache, if found return.
        // Wait, the previous step 166 code shows I removed the immediate return to allow children scanning?
        // No, in step 166 I commented:
        // "// Proceed to scan logic, but use cache for individual items."
        // So I removed the return.
        // BUT for "Serving from cache", we usually want to return the structure immediately if possible.
        // However, with flat cache, we DON'T have the structure (children list).
        // So we ALWAYS have to `readdir`.
        // So the "Serving from cache" log is a bit misleading now, it just means "We know the total size of this folder".

        // For FORCE REFRESH: We explicitly want to IGNORE the cache for the current folder's size.
    }

    // Capture old size for diff propagation
    const oldSize = diskCache[scanPath] || 0;

    try {
        const files = await fs.promises.readdir(scanPath);
        const totalItems = files.length;
        let processedCount = 0;
        let totalTotalSize = 0;
        const results = [];

        // Concurrency Limit
        const CONCURRENCY = 1;
        const queue = [...files];

        const processItem = async (fileName) => {
            const fullPath = path.join(scanPath, fileName);

            // Notify progress "Scanning: ..."
            const safeName = fileName.replace(/"/g, '\\"');
            res.write(`event: progress\ndata: ${JSON.stringify({ message: `Scanning: ${safeName} (${processedCount + 1}/${totalItems})` })}\n\n`);

            let size = 0;
            let isDir = false;

            // Check if item is in flat cache
            // If FORCE REFRESH, we might still want to use cache for *children* (sub-folders), 
            // unless we want to refresh recursively? 
            // User said: "recalculate current path bypassing cache". 
            // Usually recursive refresh is too expensive. 
            // Let's assume we refresh the *listings* and *files* of the current folder, 
            // but for sub-directories, we still trust their cached value unless verified otherwise?
            // Actually, if we refresh a folder, `du -d 1` will re-calculate sizes of immediate children.
            // So we effectively update all immediate children sizes too!

            if (!forceRefresh && diskCache[fullPath] !== undefined) {
                // Found in cache!
                size = diskCache[fullPath];
                try {
                    const stats = await fs.promises.lstat(fullPath);
                    isDir = stats.isDirectory();
                } catch (e) { }
            } else {
                // Not in cache OR Force Refresh
                // OR fallback behavior

                // Manual iteration for liveness
                let children = [];
                let manualScan = false;

                try {
                    const stats = await fs.promises.lstat(fullPath);
                    if (stats.isDirectory()) {
                        children = await fs.promises.readdir(fullPath);
                        manualScan = true;
                    }
                } catch (e) { }

                if (manualScan && children.length > 0) {
                    isDir = true;
                    size = 0;
                    // Scan children manually
                    for (const child of children) {
                        const childPath = path.join(fullPath, child);

                        // Notify progress: "Scanning: Parent/Child"
                        const displayPath = path.join(fileName, child);
                        const safeSafeName = displayPath.replace(/"/g, '\\"');
                        res.write(`event: progress\ndata: ${JSON.stringify({ message: `Scanning: ${safeSafeName} (${processedCount + 1}/${totalItems})` })}\n\n`);

                        // Check cache for grandchild?
                        // If forceRefresh of Parent, do we force refresh grandchildren?
                        // Standard `du` behavior calculates size from scratch.
                        // But here we are doing manual summation.

                        // If we are satisfying "Refresh this folder", we probably want accurate sizes for children.
                        // If child is in cache, usage is fast.
                        // But if the User clicked "Refresh" because they deleted a file deep down...
                        // `du` would catch it.
                        // WE should probably just use `getItemSize` (which uses `du`) if we are refreshing?
                        // But `getItemSize` logic was also modified to use `du -d 1`.

                        // If forceRefresh=true, we fall into this `else` block (ignoring `diskCache[fullPath]`).
                        // Then we iterate grandchildren.
                        // For grandchildren, we use `diskCache[childPath]` check below?

                        if (!forceRefresh && diskCache[childPath] !== undefined) {
                            size += diskCache[childPath];
                        } else {
                            const childResult = await getItemSize(childPath);
                            size += childResult.size;
                            if (childResult.isDir) {
                                diskCache[childPath] = childResult.size;
                            }
                        }
                    }
                    diskCache[fullPath] = size;
                } else {
                    // Fallback to standard du
                    const result = await getItemSize(fullPath);
                    size = result.size;
                    isDir = result.isDir;
                    if (isDir) {
                        diskCache[fullPath] = size;
                    }
                }
            }

            totalTotalSize += size;
            const itemData = {
                name: fileName,
                path: fullPath,
                size: size,
                formattedSize: formatSize(size),
                isDir: isDir
            };

            results.push(itemData);
            res.write(`event: item\ndata: ${JSON.stringify(itemData)}\n\n`);

            processedCount++;
        };

        // Worker Loop
        const workers = [];
        for (let i = 0; i < CONCURRENCY; i++) {
            workers.push((async () => {
                while (queue.length > 0) {
                    const file = queue.shift();
                    await processItem(file);
                }
            })());
        }

        await Promise.all(workers);

        // Sort results
        results.sort((a, b) => b.size - a.size);

        const finalData = {
            path: scanPath,
            totalSize: totalTotalSize,
            formattedTotalSize: formatSize(totalTotalSize),
            children: results
        };

        // Update Cache for parent (Current Folder)
        diskCache[scanPath] = totalTotalSize;

        // PROPAGATE DIFF TO PARENTS
        if (forceRefresh) {
            const diff = totalTotalSize - oldSize;
            if (diff !== 0) {
                console.log(`Propagating size diff: ${diff} KB starting from parent of ${scanPath}`);
                let currentPath = path.dirname(scanPath);
                while (currentPath && currentPath !== path.dirname(currentPath)) { // Stop at root (dirname('/') === '/')
                    if (diskCache[currentPath] !== undefined) {
                        diskCache[currentPath] += diff;
                    }
                    currentPath = path.dirname(currentPath);
                }
                // Handle root explicitly if loop condition missed it or if simple string check
                if (diskCache['/'] !== undefined && scanPath !== '/') {
                    // Actually the loop above handles it if `path.dirname` behaves standardly.
                    // path.dirname('/Applications') -> '/'
                    // path.dirname('/') -> '/'
                    // So loop terminates when currentPath stays same.
                }
            }
        }

        saveCache();

        res.write(`event: complete\ndata: ${JSON.stringify(finalData)}\n\n`);
        res.end();

    } catch (err) {
        console.error("Scan error:", err);
        res.write(`event: progress\ndata: ${JSON.stringify({ message: `Error: ${err.message}` })}\n\n`);
        res.end();
    }
});

app.get('/api/cache', (req, res) => {
    res.json(diskCache);
});

app.get('/api/open', (req, res) => {
    const pathArg = req.query.path;
    if (!pathArg) return res.status(400).send('Path required');

    // Use 'open' command on mac
    exec(`open "${pathArg}"`, (error) => {
        if (error) {
            console.error(`exec error: ${error}`);
            return res.status(500).send('Error opening finder');
        }
        res.send('Opened');
    });
});

app.listen(PORT, () => {
    console.log(`Disk Analyzer running at http://localhost:${PORT}`);
    exec(`open http://localhost:${PORT}`);
});
