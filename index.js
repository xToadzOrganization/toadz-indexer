// ============================================================
// TOADZSTAKE STAKER TRACKING - ADD TO toadz-indexer/index.js
// ============================================================

// ==================== STEP 1: Add after line 28 (after FALLBACK_RPC) ====================

const FLARE_RPC = 'https://flare-api.flare.network/ext/C/rpc';
const TOADZSTAKE_ADDRESS = '0xef3722efB994bb7657616763ffD7e70f5E1b2999';
let flareLastBlock = 54410000; // Block when ToadzStake deployed


// ==================== STEP 2: Add to db.exec (after artist_nfts table, around line 203) ====================

    CREATE TABLE IF NOT EXISTS toadzstake_stakers (
        address TEXT PRIMARY KEY,
        block_number INTEGER,
        created_at INTEGER DEFAULT (strftime('%s', 'now'))
    );
    
    CREATE TABLE IF NOT EXISTS flare_sync_state (
        id INTEGER PRIMARY KEY,
        last_block INTEGER NOT NULL
    );


// ==================== STEP 3: Add before app.listen (around line 2580) ====================

// ==================== FLARE TOADZSTAKE STAKER INDEXING ====================

const TOADZSTAKE_ABI = [
    'event Deposited(address indexed user, uint256 wflrAmount, uint256 pondAmount, uint256 lockDays, uint256 multiplier)'
];

async function indexFlareStakers() {
    try {
        const flareProvider = new ethers.providers.JsonRpcProvider(FLARE_RPC);
        const contract = new ethers.Contract(TOADZSTAKE_ADDRESS, TOADZSTAKE_ABI, flareProvider);
        
        // Get last synced block
        const syncRow = db.prepare('SELECT last_block FROM flare_sync_state WHERE id = 1').get();
        let fromBlock = syncRow ? syncRow.last_block + 1 : flareLastBlock;
        
        const currentBlock = await flareProvider.getBlockNumber();
        
        // Flare RPC limits to 30 blocks per query
        const MAX_RANGE = 29;
        let newStakers = 0;
        
        while (fromBlock < currentBlock) {
            const toBlock = Math.min(fromBlock + MAX_RANGE, currentBlock);
            
            try {
                const events = await contract.queryFilter('Deposited', fromBlock, toBlock);
                
                for (const event of events) {
                    const staker = event.args.user.toLowerCase();
                    db.prepare(`
                        INSERT OR IGNORE INTO toadzstake_stakers (address, block_number)
                        VALUES (?, ?)
                    `).run(staker, event.blockNumber);
                    newStakers++;
                }
            } catch (e) {
                console.log(`Flare block ${fromBlock}-${toBlock} failed:`, e.message);
            }
            
            fromBlock = toBlock + 1;
        }
        
        // Update sync state
        db.prepare(`
            INSERT OR REPLACE INTO flare_sync_state (id, last_block)
            VALUES (1, ?)
        `).run(currentBlock);
        
        if (newStakers > 0) {
            console.log(`Indexed ${newStakers} new ToadzStake stakers up to block ${currentBlock}`);
        }
    } catch (err) {
        console.error('Flare staker indexing failed:', err.message);
    }
}

// Get all stakers endpoint
app.get('/api/stakers', (req, res) => {
    try {
        const stakers = db.prepare('SELECT address FROM toadzstake_stakers ORDER BY block_number ASC').all();
        res.json(stakers.map(s => s.address));
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Get staker count
app.get('/api/stakers/count', (req, res) => {
    try {
        const count = db.prepare('SELECT COUNT(*) as count FROM toadzstake_stakers').get();
        res.json({ count: count.count });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});


// ==================== STEP 4: Add inside app.listen callback (after updateFloors()) ====================

    // Index Flare ToadzStake stakers
    indexFlareStakers();
    setInterval(indexFlareStakers, 60000); // Every minute


// ============================================================
// COMPLETE UPDATED app.listen SHOULD LOOK LIKE:
// ============================================================

app.listen(PORT, () => {
    console.log(`Toadz Indexer running on port ${PORT}`);
    
    indexEvents();
    updateFloors();
    
    // Index Flare ToadzStake stakers
    indexFlareStakers();
    
    setInterval(indexEvents, POLL_INTERVAL);
    setInterval(notifyOfferSellers, 30000);
    setInterval(updateFloors, 300000);
    setInterval(indexFlareStakers, 60000); // Every minute
});
