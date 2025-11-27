// ==============================
const express = require('express');
const cors = require('cors');
const { ethers } = require('ethers');
const Database = require('better-sqlite3');

// ==================== CONFIG ====================
const PORT = process.env.PORT || 8080;
// Private node - no rate limits!
const RPC_URL = 'http://135.181.215.126:9650/ext/bc/C/rpc';
const FALLBACK_RPC = 'https://songbird-api.flare.network/ext/C/rpc';
const POLL_INTERVAL = 5000; // 5 seconds - faster with private node

// Contract addresses
const CONTRACTS = {
    marketplace: '0xc99c294224BCB259F1860F0EeaABa664b29d1633',
    nftStaking: '0xAC3E3651a4FA87784dee501a962aBD5005EebB64',
    pondToken: '0x39fec3F97668e393862Dbb3C442f3Dd3d5016D69'
};

// Collections
const COLLECTIONS = {
    '0x35afb6Ba51839dEDD33140A3b704b39933D1e642': { name: 'sToadz', symbol: 'STOADZ', supply: 10000 },
    '0x91Aa85a172DD3e7EEA4ad1A4B33E90cbF3B99ed8': { name: 'Luxury Lofts', symbol: 'LOFT', supply: 10000 },
    '0x360f8B7d9530F55AB8E52394E6527935635f51E7': { name: 'Songbird City', symbol: 'SBCITY', supply: 10000 }
};

// Event signatures
const EVENT_SIGS = {
    Listed: 'Listed(address,uint256,address,uint256,uint256)',
    Unlisted: 'Unlisted(address,uint256,address)',
    Sold: 'Sold(address,uint256,address,address,uint256,uint256)',
    OfferMade: 'OfferMade(address,uint256,address,uint256,uint256,uint256)',
    OfferAccepted: 'OfferAccepted(address,uint256,address,address,uint256,uint256)',
    OfferCancelled: 'OfferCancelled(address,uint256,address)',
    Staked: 'Staked(address,address,uint256)'
};

// ==================== DATABASE ====================
const fs = require('fs');
const dbPath = fs.existsSync('/data') ? '/data/indexer.db' : './indexer.db';
console.log(`Using database path: ${dbPath}`);

let db;
try {
    db = new Database(dbPath);
    console.log('Database connected successfully');
} catch (err) {
    console.error('Database connection failed:', err.message);
    console.log('Falling back to in-memory database');
    db = new Database(':memory:');
}
let forceStartBlock = null; // In-memory flag for immediate reset

// Initialize tables
db.exec(`
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tx_hash TEXT NOT NULL,
        block_number INTEGER NOT NULL,
        timestamp INTEGER NOT NULL,
        event_type TEXT NOT NULL,
        collection TEXT,
        token_id INTEGER,
        from_address TEXT,
        to_address TEXT,
        price_sgb TEXT,
        price_pond TEXT,
        created_at INTEGER DEFAULT (strftime('%s', 'now')),
        UNIQUE(tx_hash, event_type, collection, token_id)
    );
    
    CREATE TABLE IF NOT EXISTS notifications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_address TEXT NOT NULL,
        event_id INTEGER,
        type TEXT NOT NULL,
        urgency TEXT NOT NULL,
        title TEXT NOT NULL,
        message TEXT,
        collection TEXT,
        token_id INTEGER,
        is_read INTEGER DEFAULT 0,
        created_at INTEGER DEFAULT (strftime('%s', 'now')),
        FOREIGN KEY (event_id) REFERENCES events(id)
    );
    
    CREATE TABLE IF NOT EXISTS sync_state (
        id INTEGER PRIMARY KEY,
        last_block INTEGER NOT NULL
    );
    
    CREATE TABLE IF NOT EXISTS floors (
        collection TEXT PRIMARY KEY,
        floor_sgb TEXT,
        floor_pond TEXT,
        updated_at INTEGER
    );
    
    CREATE TABLE IF NOT EXISTS nft_ownership (
        collection TEXT NOT NULL,
        token_id INTEGER NOT NULL,
        owner TEXT NOT NULL,
        updated_at INTEGER DEFAULT (strftime('%s', 'now')),
        PRIMARY KEY (collection, token_id)
    );
    
    CREATE TABLE IF NOT EXISTS ownership_sync (
        collection TEXT PRIMARY KEY,
        last_token_id INTEGER DEFAULT 0,
        completed INTEGER DEFAULT 0,
        updated_at INTEGER DEFAULT (strftime('%s', 'now'))
    );
    
    CREATE INDEX IF NOT EXISTS idx_events_collection ON events(collection);
    CREATE INDEX IF NOT EXISTS idx_events_block ON events(block_number DESC);
    CREATE INDEX IF NOT EXISTS idx_events_from ON events(from_address);
    CREATE INDEX IF NOT EXISTS idx_events_to ON events(to_address);
    CREATE INDEX IF NOT EXISTS idx_notifications_user ON notifications(user_address, is_read);
    CREATE INDEX IF NOT EXISTS idx_nft_owner ON nft_ownership(owner);
`);

// Prepared statements
const stmts = {
    insertEvent: db.prepare(`
        INSERT OR IGNORE INTO events (tx_hash, block_number, timestamp, event_type, collection, token_id, from_address, to_address, price_sgb, price_pond)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `),
    insertNotification: db.prepare(`
        INSERT INTO notifications (user_address, event_id, type, urgency, title, message, collection, token_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `),
    getLastBlock: db.prepare('SELECT last_block FROM sync_state WHERE id = 1'),
    setLastBlock: db.prepare('INSERT OR REPLACE INTO sync_state (id, last_block) VALUES (1, ?)'),
    getRecentEvents: db.prepare(`
        SELECT * FROM events ORDER BY block_number DESC, id DESC LIMIT ?
    `),
    getCollectionEvents: db.prepare(`
        SELECT * FROM events WHERE collection = ? ORDER BY block_number DESC LIMIT ?
    `),
    getUserEvents: db.prepare(`
        SELECT * FROM events WHERE from_address = ? OR to_address = ? ORDER BY block_number DESC LIMIT ?
    `),
    getUserNotifications: db.prepare(`
        SELECT * FROM notifications WHERE user_address = ? ORDER BY created_at DESC LIMIT ?
    `),
    getUnreadNotifications: db.prepare(`
        SELECT * FROM notifications WHERE user_address = ? AND is_read = 0 ORDER BY created_at DESC
    `),
    markNotificationsRead: db.prepare(`
        UPDATE notifications SET is_read = 1 WHERE user_address = ? AND is_read = 0
    `),
    updateFloor: db.prepare(`
        INSERT OR REPLACE INTO floors (collection, floor_sgb, floor_pond, updated_at)
        VALUES (?, ?, ?, strftime('%s', 'now'))
    `),
    getFloors: db.prepare('SELECT * FROM floors'),
    
    // NFT ownership
    upsertOwnership: db.prepare(`
        INSERT OR REPLACE INTO nft_ownership (collection, token_id, owner, updated_at)
        VALUES (?, ?, ?, strftime('%s', 'now'))
    `),
    getUserNfts: db.prepare(`
        SELECT collection, token_id FROM nft_ownership WHERE LOWER(owner) = LOWER(?) ORDER BY collection, token_id
    `),
    getOwnershipSync: db.prepare('SELECT * FROM ownership_sync WHERE collection = ?'),
    upsertOwnershipSync: db.prepare(`
        INSERT OR REPLACE INTO ownership_sync (collection, last_token_id, completed, updated_at)
        VALUES (?, ?, ?, strftime('%s', 'now'))
    `)
};

// ==================== PROVIDER ====================
const provider = new ethers.providers.JsonRpcProvider(RPC_URL);

// ABIs (minimal for event parsing) - MUST MATCH DEPLOYED CONTRACT
const MARKETPLACE_ABI = [
    'event Listed(address indexed nftContract, uint256 indexed tokenId, address indexed seller, uint256 price, uint256 feeAmount)',
    'event Unlisted(address indexed nftContract, uint256 indexed tokenId, address indexed seller)',
    'event Sold(address indexed nftContract, uint256 indexed tokenId, address indexed buyer, address seller, uint256 price, uint256 feeAmount)',
    'event OfferMade(address indexed nftContract, uint256 indexed tokenId, address indexed buyer, uint256 amount, uint256 expiry)',
    'event OfferAccepted(address indexed nftContract, uint256 indexed tokenId, address indexed buyer, address seller, uint256 amount)',
    'event OfferCancelled(address indexed nftContract, uint256 indexed tokenId, address indexed buyer)',
    'function getListing(address collection, uint256 tokenId) view returns (address seller, uint256 priceSGB, uint256 pricePOND, bool active)',
    'function getActiveListings(address collection) view returns (uint256[])'
];

const STAKING_ABI = [
    'event Staked(address indexed user, address indexed collection, uint256 tokenId)',
    'event Unstaked(address indexed user, address indexed collection, uint256 tokenId)',
    'event RewardsClaimed(address indexed user, uint256 amount)'
];

const ERC721_ABI = [
    'function ownerOf(uint256 tokenId) view returns (address)',
    'function balanceOf(address owner) view returns (uint256)'
];

const marketplace = new ethers.Contract(CONTRACTS.marketplace, MARKETPLACE_ABI, provider);
const staking = new ethers.Contract(CONTRACTS.nftStaking, STAKING_ABI, provider);

// ==================== HELPER FUNCTIONS ====================
// FIXED: Added null check to prevent crash
function getCollectionName(address) {
    if (!address) return 'Unknown';
    try {
        const col = COLLECTIONS[address] || COLLECTIONS[address.toLowerCase()];
        return col ? col.name : 'NFT';
    } catch (err) {
        return 'NFT';
    }
}

function getTimeAgo(timestamp) {
    if (!timestamp) return 'Unknown';
    try {
        const now = Math.floor(Date.now() / 1000);
        const diff = now - timestamp;
        
        if (diff < 60) return 'Just now';
        if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
        if (diff < 86400) return Math.floor(diff / 3600) + 'h ago';
        return Math.floor(diff / 86400) + 'd ago';
    } catch (err) {
        return 'Unknown';
    }
}

// FIXED: Added null checks for all properties
function formatEvent(e) {
    if (!e) return null;
    return {
        ...e,
        collection_name: getCollectionName(e.collection),
        time_ago: getTimeAgo(e.timestamp)
    };
}

// ==================== INDEXER ====================
async function getStartBlock() {
    // Check in-memory flag first (set by /admin/reset)
    if (forceStartBlock !== null) {
        const block = forceStartBlock;
        forceStartBlock = null; // Clear flag
        return block;
    }
    
    const row = stmts.getLastBlock.get();
    if (row) return row.last_block + 1;
    
    // Start from ~3 days ago if fresh (gives more history)
    const current = await provider.getBlockNumber();
    return Math.max(0, current - 17280); // ~3 days at 15s blocks
}

// Helper to delay between requests
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function indexEvents() {
    try {
        const fromBlock = await getStartBlock();
        const currentBlock = await provider.getBlockNumber();
        
        // Only 10 blocks at a time for Songbird RPC limits
        const toBlock = Math.min(fromBlock + 500, currentBlock);
        
        if (fromBlock > currentBlock) {
            return; // Already synced
        }
        
        console.log(`Indexing blocks ${fromBlock} to ${toBlock}...`);
        
        // Query events sequentially with delays to avoid rate limits
        let listedEvents = [];
        let unlistedEvents = [];
        let soldEvents = [];
        let offerMadeEvents = [];
        let offerAcceptedEvents = [];
        let stakedEvents = [];
        let rewardsClaimedEvents = [];
        
        try {
            listedEvents = await marketplace.queryFilter(marketplace.filters.Listed(), fromBlock, toBlock);
            console.log(`  Listed query: ${listedEvents.length} events`);
        } catch (err) {
            console.log(`  Listed query FAILED: ${err.message}`);
        }
        await delay(100);
        
        try {
            unlistedEvents = await marketplace.queryFilter(marketplace.filters.Unlisted(), fromBlock, toBlock);
        } catch (err) {
            console.log(`  Unlisted query FAILED: ${err.message}`);
        }
        await delay(100);
        
        try {
            soldEvents = await marketplace.queryFilter(marketplace.filters.Sold(), fromBlock, toBlock);
            console.log(`  Sold query: ${soldEvents.length} events`);
        } catch (err) {
            console.log(`  Sold query FAILED: ${err.message}`);
        }
        await delay(100);
        
        try {
            offerMadeEvents = await marketplace.queryFilter(marketplace.filters.OfferMade(), fromBlock, toBlock);
        } catch (err) {
            console.log(`  OfferMade query FAILED: ${err.message}`);
        }
        await delay(100);
        
        try {
            offerAcceptedEvents = await marketplace.queryFilter(marketplace.filters.OfferAccepted(), fromBlock, toBlock);
        } catch (err) {
            console.log(`  OfferAccepted query FAILED: ${err.message}`);
        }
        await delay(100);
        
        try {
            stakedEvents = await staking.queryFilter(staking.filters.Staked(), fromBlock, toBlock);
        } catch (err) {
            console.log(`  Staked query FAILED: ${err.message}`);
        }
        await delay(100);
        
        try {
            rewardsClaimedEvents = await staking.queryFilter(staking.filters.RewardsClaimed(), fromBlock, toBlock);
        } catch (err) {
            console.log(`  RewardsClaimed query FAILED: ${err.message}`);
        }
        
        // Get block timestamps (batch)
        const blockNumbers = new Set();
        [...listedEvents, ...unlistedEvents, ...soldEvents, ...offerMadeEvents, ...offerAcceptedEvents, ...stakedEvents, ...rewardsClaimedEvents]
            .forEach(e => blockNumbers.add(e.blockNumber));
        
        const timestamps = {};
        for (const bn of blockNumbers) {
            try {
                const block = await provider.getBlock(bn);
                timestamps[bn] = block.timestamp;
            } catch {
                timestamps[bn] = Math.floor(Date.now() / 1000);
            }
        }
        
        // Process events in transaction
        const processEvents = db.transaction(() => {
            // Listed events
            for (const e of listedEvents) {
                try {
                    const collection = e.args.nftContract;
                    const tokenId = e.args.tokenId.toNumber();
                    const seller = e.args.seller;
                    const price = ethers.utils.formatEther(e.args.price);
                    
                    if (!collection || !seller) continue; // Skip invalid events
                    
                    const result = stmts.insertEvent.run(
                        e.transactionHash, e.blockNumber, timestamps[e.blockNumber],
                        'listed', collection, tokenId, seller, null, price, '0'
                    );
                    
                    if (result.changes > 0) {
                        // Green notification for seller
                        stmts.insertNotification.run(
                            seller.toLowerCase(), result.lastInsertRowid, 'listed', 'green',
                            'NFT Listed', `Your ${getCollectionName(collection)} #${tokenId} is now listed`,
                            collection, tokenId
                        );
                    }
                } catch (err) {
                    console.log(`Error processing listed event: ${err.message}`);
                }
            }
            
            // Sold events
            for (const e of soldEvents) {
                try {
                    const collection = e.args.nftContract;
                    const tokenId = e.args.tokenId.toNumber();
                    const seller = e.args.seller;
                    const buyer = e.args.buyer;
                    const price = ethers.utils.formatEther(e.args.price);
                    
                    if (!collection || !seller || !buyer) continue; // Skip invalid events
                    
                    const result = stmts.insertEvent.run(
                        e.transactionHash, e.blockNumber, timestamps[e.blockNumber],
                        'sold', collection, tokenId, seller, buyer, price, '0'
                    );
                    
                    if (result.changes > 0) {
                        const priceStr = `${price} SGB`;
                        
                        // RED notification for seller (money incoming!)
                        stmts.insertNotification.run(
                            seller.toLowerCase(), result.lastInsertRowid, 'sold', 'red',
                            'NFT Sold!', `Your ${getCollectionName(collection)} #${tokenId} sold for ${priceStr}`,
                            collection, tokenId
                        );
                        
                        // Green notification for buyer
                        stmts.insertNotification.run(
                            buyer.toLowerCase(), result.lastInsertRowid, 'purchased', 'green',
                            'NFT Purchased', `You bought ${getCollectionName(collection)} #${tokenId}`,
                            collection, tokenId
                        );
                    }
                } catch (err) {
                    console.log(`Error processing sold event: ${err.message}`);
                }
            }
            
            // Offer made events
            for (const e of offerMadeEvents) {
                try {
                    const collection = e.args.nftContract;
                    const tokenId = e.args.tokenId.toNumber();
                    const buyer = e.args.buyer;
                    const amount = ethers.utils.formatEther(e.args.amount);
                    
                    if (!collection || !buyer) continue; // Skip invalid events
                    
                    const result = stmts.insertEvent.run(
                        e.transactionHash, e.blockNumber, timestamps[e.blockNumber],
                        'offer_made', collection, tokenId, buyer, null, amount, '0'
                    );
                    
                    // We need to find the seller to notify them - query the listing
                    if (result.changes > 0) {
                        // Note: seller notification happens via separate lookup
                        // For now, we'll handle this in a follow-up query
                    }
                } catch (err) {
                    console.log(`Error processing offer_made event: ${err.message}`);
                }
            }
            
            // Offer accepted events
            for (const e of offerAcceptedEvents) {
                try {
                    const collection = e.args.nftContract;
                    const tokenId = e.args.tokenId.toNumber();
                    const seller = e.args.seller;
                    const buyer = e.args.buyer;
                    const amount = ethers.utils.formatEther(e.args.amount);
                    
                    if (!collection || !seller || !buyer) continue; // Skip invalid events
                    
                    const result = stmts.insertEvent.run(
                        e.transactionHash, e.blockNumber, timestamps[e.blockNumber],
                        'offer_accepted', collection, tokenId, seller, buyer, amount, '0'
                    );
                    
                    if (result.changes > 0) {
                        const priceStr = `${amount} SGB`;
                        
                        // RED for seller
                        stmts.insertNotification.run(
                            seller.toLowerCase(), result.lastInsertRowid, 'offer_accepted', 'red',
                            'Offer Accepted!', `You sold ${getCollectionName(collection)} #${tokenId} for ${priceStr}`,
                            collection, tokenId
                        );
                        
                        // Green for buyer
                        stmts.insertNotification.run(
                            buyer.toLowerCase(), result.lastInsertRowid, 'offer_accepted', 'green',
                            'Offer Accepted', `Your offer on ${getCollectionName(collection)} #${tokenId} was accepted`,
                            collection, tokenId
                        );
                    }
                } catch (err) {
                    console.log(`Error processing offer_accepted event: ${err.message}`);
                }
            }
            
            // Unlisted events
            for (const e of unlistedEvents) {
                try {
                    if (!e.args.nftContract || !e.args.seller) continue;
                    stmts.insertEvent.run(
                        e.transactionHash, e.blockNumber, timestamps[e.blockNumber],
                        'unlisted', e.args.nftContract, e.args.tokenId.toNumber(),
                        e.args.seller, null, '0', '0'
                    );
                } catch (err) {
                    console.log(`Error processing unlisted event: ${err.message}`);
                }
            }
            
            // Staked events
            for (const e of stakedEvents) {
                try {
                    const user = e.args.user;
                    const collection = e.args.collection;
                    const tokenId = e.args.tokenId.toNumber();
                    
                    if (!user || !collection) continue; // Skip invalid events
                    
                    const result = stmts.insertEvent.run(
                        e.transactionHash, e.blockNumber, timestamps[e.blockNumber],
                        'staked', collection, tokenId, user, null, '0', '0'
                    );
                    
                    if (result.changes > 0) {
                        stmts.insertNotification.run(
                            user.toLowerCase(), result.lastInsertRowid, 'staked', 'green',
                            'NFT Staked', `${getCollectionName(collection)} #${tokenId} is now earning rewards`,
                            collection, tokenId
                        );
                    }
                } catch (err) {
                    console.log(`Error processing staked event: ${err.message}`);
                }
            }
            
            // RewardsClaimed events
            for (const e of rewardsClaimedEvents) {
                try {
                    const user = e.args.user;
                    const amount = ethers.utils.formatEther(e.args.amount);
                    
                    if (!user) continue; // Skip invalid events
                    
                    const result = stmts.insertEvent.run(
                        e.transactionHash, e.blockNumber, timestamps[e.blockNumber],
                        'rewards_claimed', null, null, user, null, '0', amount
                    );
                    
                    if (result.changes > 0) {
                        stmts.insertNotification.run(
                            user.toLowerCase(), result.lastInsertRowid, 'rewards_claimed', 'green',
                            'POND Claimed', `You claimed ${parseFloat(amount).toLocaleString()} POND`,
                            null, null
                        );
                    }
                } catch (err) {
                    console.log(`Error processing rewards_claimed event: ${err.message}`);
                }
            }
            
            // Update last synced block
            stmts.setLastBlock.run(toBlock);
        });
        
        processEvents();
        
        const totalEvents = listedEvents.length + soldEvents.length + offerMadeEvents.length + 
                          offerAcceptedEvents.length + unlistedEvents.length + stakedEvents.length + rewardsClaimedEvents.length;
        if (totalEvents > 0) {
            console.log(`Found ${totalEvents} events (${listedEvents.length} listed, ${soldEvents.length} sold, ${stakedEvents.length} staked, ${rewardsClaimedEvents.length} claimed)`);
        }
        
        // Delay before next iteration - faster with private node
        await delay(500);
        
    } catch (err) {
        console.error('Indexer error:', err.message);
        await delay(2000); // Wait on error
    }
}

// Notify sellers of new offers (separate function to handle async listing lookup)
async function notifyOfferSellers() {
    try {
        // Get recent offer_made events without seller notifications
        const recentOffers = db.prepare(`
            SELECT e.* FROM events e
            LEFT JOIN notifications n ON n.event_id = e.id AND n.type = 'offer_received'
            WHERE e.event_type = 'offer_made' AND n.id IS NULL
            ORDER BY e.id DESC LIMIT 20
        `).all();
        
        for (const offer of recentOffers) {
            try {
                if (!offer.collection) continue; // Skip if no collection
                const [seller, , , active] = await marketplace.getListing(offer.collection, offer.token_id);
                if (active && seller !== ethers.constants.AddressZero) {
                    const priceStr = parseFloat(offer.price_sgb) > 0 ? `${offer.price_sgb} SGB` : `${offer.price_pond} POND`;
                    
                    stmts.insertNotification.run(
                        seller.toLowerCase(), offer.id, 'offer_received', 'red',
                        'New Offer!', `Offer of ${priceStr} on ${getCollectionName(offer.collection)} #${offer.token_id}`,
                        offer.collection, offer.token_id
                    );
                }
            } catch {}
        }
    } catch (err) {
        console.error('Offer notification error:', err.message);
    }
}

// Update floor prices
async function updateFloors() {
    try {
        for (const [address, col] of Object.entries(COLLECTIONS)) {
            try {
                const activeListings = await marketplace.getActiveListings(address);
                
                let floorSGB = null;
                let floorPOND = null;
                
                for (const tokenId of activeListings.slice(0, 50)) { // Check first 50
                    try {
                        const [, priceSGB, pricePOND, active] = await marketplace.getListing(address, tokenId);
                        if (!active) continue;
                        
                        const sgb = parseFloat(ethers.utils.formatEther(priceSGB));
                        const pond = parseFloat(ethers.utils.formatEther(pricePOND));
                        
                        if (sgb > 0 && (floorSGB === null || sgb < floorSGB)) {
                            floorSGB = sgb;
                        }
                        if (pond > 0 && (floorPOND === null || pond < floorPOND)) {
                            floorPOND = pond;
                        }
                    } catch {}
                }
                
                stmts.updateFloor.run(
                    address,
                    floorSGB ? floorSGB.toString() : null,
                    floorPOND ? floorPOND.toString() : null
                );
                
            } catch (err) {
                console.log(`Floor update failed for ${col.name}:`, err.message);
            }
        }
    } catch (err) {
        console.error('Floor update error:', err.message);
    }
}

// ==================== NFT OWNERSHIP INDEXING ====================
let isIndexingOwnership = false;

async function indexNftOwnership() {
    if (isIndexingOwnership) {
        console.log('Ownership indexing already in progress, skipping...');
        return;
    }
    
    isIndexingOwnership = true;
    console.log('Starting NFT ownership indexing...');
    
    try {
        for (const [address, col] of Object.entries(COLLECTIONS)) {
            const contract = new ethers.Contract(address, ERC721_ABI, provider);
            const supply = col.supply || 10000;
            
            // Get sync state
            let syncState = stmts.getOwnershipSync.get(address);
            let startToken = syncState ? syncState.last_token_id + 1 : 1;
            
            // If completed, just do a refresh of random samples
            if (syncState && syncState.completed) {
                console.log(`${col.name}: Already indexed, refreshing...`);
                // Refresh in batches - check 500 random tokens
                const tokensToCheck = [];
                for (let i = 0; i < 500; i++) {
                    tokensToCheck.push(Math.floor(Math.random() * supply) + 1);
                }
                
                const batchSize = 50;
                for (let i = 0; i < tokensToCheck.length; i += batchSize) {
                    const batch = tokensToCheck.slice(i, i + batchSize);
                    const results = await Promise.all(
                        batch.map(id => 
                            contract.ownerOf(id)
                                .then(owner => ({ id, owner }))
                                .catch(() => ({ id, owner: null }))
                        )
                    );
                    
                    for (const { id, owner } of results) {
                        if (owner) {
                            stmts.upsertOwnership.run(address, id, owner.toLowerCase());
                        }
                    }
                    await delay(30);
                }
                continue;
            }
            
            console.log(`${col.name}: Indexing from token ${startToken}...`);
            
            // Index in batches
            const batchSize = 50;
            for (let start = startToken; start <= supply; start += batchSize) {
                const end = Math.min(start + batchSize - 1, supply);
                const tokenIds = Array.from({ length: end - start + 1 }, (_, i) => start + i);
                
                const results = await Promise.all(
                    tokenIds.map(id => 
                        contract.ownerOf(id)
                            .then(owner => ({ id, owner }))
                            .catch(() => ({ id, owner: null }))
                    )
                );
                
                for (const { id, owner } of results) {
                    if (owner) {
                        stmts.upsertOwnership.run(address, id, owner.toLowerCase());
                    }
                }
                
                // Update sync state
                stmts.upsertOwnershipSync.run(address, end, end >= supply ? 1 : 0);
                
                // Fast with private node
                await delay(50);
                
                // Log progress every 500 tokens
                if (end % 500 === 0) {
                    console.log(`${col.name}: Indexed ${end}/${supply} tokens`);
                }
            }
            
            console.log(`${col.name}: Ownership indexing complete!`);
        }
    } catch (err) {
        console.error('Ownership indexing error:', err.message);
    } finally {
        isIndexingOwnership = false;
    }
}

// ==================== API ====================
const app = express();
app.use(cors());
app.use(express.json());

// Health check
app.get('/', (req, res) => {
    res.json({ status: 'ok', service: 'toadz-indexer' });
});

// Recent activity (all)
app.get('/activity', (req, res) => {
    try {
        const limit = Math.min(parseInt(req.query.limit) || 50, 100);
        const events = stmts.getRecentEvents.all(limit);
        res.json(events.map(formatEvent).filter(e => e !== null));
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Collection activity
app.get('/activity/:collection', (req, res) => {
    try {
        const limit = Math.min(parseInt(req.query.limit) || 50, 100);
        const events = stmts.getCollectionEvents.all(req.params.collection, limit);
        res.json(events.map(formatEvent).filter(e => e !== null));
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// User activity
// User stats
app.get('/user/:address/stats', (req, res) => {
    const addr = req.params.address.toLowerCase();
    
    try {
        // Get volume traded (as buyer)
        const buyerStats = db.prepare(`
            SELECT 
                COUNT(*) as buy_count,
                COALESCE(SUM(CAST(price_sgb AS REAL)), 0) as buy_volume_sgb
            FROM events 
            WHERE event_type = 'sold' AND to_address = ?
        `).get(addr);
        
        // Get volume sold (as seller)
        const sellerStats = db.prepare(`
            SELECT 
                COUNT(*) as sell_count,
                COALESCE(SUM(CAST(price_sgb AS REAL)), 0) as sell_volume_sgb
            FROM events 
            WHERE event_type = 'sold' AND from_address = ?
        `).get(addr);
        
        res.json({
            buyCount: buyerStats?.buy_count || 0,
            sellCount: sellerStats?.sell_count || 0,
            buyVolumeSGB: buyerStats?.buy_volume_sgb || 0,
            sellVolumeSGB: sellerStats?.sell_volume_sgb || 0,
            volumeSGB: (buyerStats?.buy_volume_sgb || 0) + (sellerStats?.sell_volume_sgb || 0)
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// User NFTs (from ownership index)
app.get('/user/:address/nfts', (req, res) => {
    try {
        const addr = req.params.address.toLowerCase();
        const nfts = stmts.getUserNfts.all(addr);
        
        // Group by collection
        const grouped = {};
        for (const nft of nfts) {
            if (!grouped[nft.collection]) {
                const col = COLLECTIONS[nft.collection] || COLLECTIONS[nft.collection.toLowerCase()];
                grouped[nft.collection] = {
                    collection: nft.collection,
                    name: col?.name || 'Unknown',
                    tokenIds: []
                };
            }
            grouped[nft.collection].tokenIds.push(nft.token_id);
        }
        
        res.json({
            total: nfts.length,
            collections: Object.values(grouped)
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.get('/user/:address/activity', (req, res) => {
    try {
        const limit = Math.min(parseInt(req.query.limit) || 50, 100);
        const addr = req.params.address.toLowerCase();
        const events = stmts.getUserEvents.all(addr, addr, limit);
        res.json(events.map(formatEvent).filter(e => e !== null));
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// User notifications
app.get('/user/:address/notifications', (req, res) => {
    try {
        const limit = Math.min(parseInt(req.query.limit) || 50, 100);
        const addr = req.params.address.toLowerCase();
        const notifications = stmts.getUserNotifications.all(addr, limit);
        res.json(notifications);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Unread notification counts
app.get('/user/:address/notifications/unread', (req, res) => {
    try {
        const addr = req.params.address.toLowerCase();
        const notifications = stmts.getUnreadNotifications.all(addr);
        
        const counts = { red: 0, green: 0, total: 0 };
        for (const n of notifications) {
            if (n.urgency && counts.hasOwnProperty(n.urgency)) {
                counts[n.urgency]++;
            }
            counts.total++;
        }
        
        res.json({ counts, notifications });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Mark notifications read
app.post('/user/:address/notifications/read', (req, res) => {
    try {
        const addr = req.params.address.toLowerCase();
        stmts.markNotificationsRead.run(addr);
        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Clear all notifications for user
app.post('/user/:address/notifications/clear', (req, res) => {
    try {
        const addr = req.params.address.toLowerCase();
        db.prepare('DELETE FROM notifications WHERE user_address = ?').run(addr);
        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Floor prices
app.get('/floors', (req, res) => {
    try {
        const floors = stmts.getFloors.all();
        const result = {};
        for (const f of floors) {
            result[f.collection] = {
                floor_sgb: f.floor_sgb,
                floor_pond: f.floor_pond,
                updated_at: f.updated_at
            };
        }
        res.json(result);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Collection stats
app.get('/collection/:address/stats', (req, res) => {
    const address = req.params.address;
    
    try {
        const stats = db.prepare(`
            SELECT 
                COUNT(*) as total_sales,
                COALESCE(SUM(CAST(price_sgb AS REAL)), 0) as volume_sgb,
                COALESCE(SUM(CAST(price_pond AS REAL)), 0) as volume_pond
            FROM events
            WHERE event_type = 'sold' AND LOWER(collection) = LOWER(?)
        `).get(address);
        
        // Get floor from floors table
        const floor = db.prepare('SELECT floor_sgb, floor_pond FROM floors WHERE LOWER(collection) = LOWER(?)').get(address);
        
        res.json({
            totalSales: stats?.total_sales || 0,
            volumeSGB: stats?.volume_sgb || 0,
            volumePOND: stats?.volume_pond || 0,
            floorSGB: floor?.floor_sgb || null,
            floorPOND: floor?.floor_pond || null
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Stats
app.get('/stats', (req, res) => {
    try {
        const stats = db.prepare(`
            SELECT 
                COUNT(*) as total_events,
                SUM(CASE WHEN event_type = 'sold' THEN 1 ELSE 0 END) as total_sales,
                SUM(CASE WHEN event_type = 'sold' THEN CAST(price_sgb AS REAL) ELSE 0 END) as volume_sgb,
                SUM(CASE WHEN event_type = 'sold' THEN CAST(price_pond AS REAL) ELSE 0 END) as volume_pond
            FROM events
        `).get();
        
        const lastBlock = stmts.getLastBlock.get();
        
        res.json({
            ...stats,
            last_indexed_block: lastBlock?.last_block || 0
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Reset and rescan from X days ago
app.post('/admin/reset/:days', async (req, res) => {
    try {
        const days = parseInt(req.params.days) || 7;
        const blocksBack = days * 5760; // ~5760 blocks per day
        const currentBlock = await provider.getBlockNumber();
        const newStartBlock = Math.max(0, currentBlock - blocksBack);
        
        // Clear database and set new start block
       db.exec('DELETE FROM notifications');
        db.exec('DELETE FROM events');
        stmts.setLastBlock.run(newStartBlock);
        
        // Set in-memory flag for immediate effect
        forceStartBlock = newStartBlock + 1;
        
        console.log(`Reset to block ${newStartBlock} (${days} days ago)`);
        res.json({ success: true, new_start_block: newStartBlock, days_back: days });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Reset and reindex NFT ownership
app.post('/admin/reindex-nfts', async (req, res) => {
    try {
        // Clear ownership tables
        db.exec('DELETE FROM nft_ownership');
        db.exec('DELETE FROM ownership_sync');
        
        // Trigger reindex (async)
        indexNftOwnership();
        
        res.json({ success: true, message: 'NFT ownership reindexing started' });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Get ownership index status
app.get('/admin/ownership-status', (req, res) => {
    try {
        const status = db.prepare('SELECT * FROM ownership_sync').all();
        const counts = db.prepare('SELECT collection, COUNT(*) as count FROM nft_ownership GROUP BY collection').all();
        
        res.json({
            syncStatus: status,
            indexedCounts: counts,
            isIndexing: isIndexingOwnership
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Leaderboard - Top Stakers
app.get('/leaderboard/stakers', (req, res) => {
    try {
        const limit = Math.min(parseInt(req.query.limit) || 25, 100);
        
        // Get stakers with their staked count
        const stakers = db.prepare(`
            SELECT 
                from_address as address,
                COUNT(*) as nfts_staked
            FROM events 
            WHERE event_type = 'staked'
            GROUP BY from_address
            ORDER BY nfts_staked DESC
            LIMIT ?
        `).all(limit);
        
        // Get claimed POND totals per address
        const claimedTotals = db.prepare(`
            SELECT 
                from_address as address,
                SUM(CAST(price_pond AS REAL)) as total_claimed
            FROM events 
            WHERE event_type = 'rewards_claimed'
            GROUP BY from_address
        `).all();
        
        // Create map for quick lookup
        const claimedMap = {};
        for (const c of claimedTotals) {
            if (c.address) {
                claimedMap[c.address.toLowerCase()] = c.total_claimed || 0;
            }
        }
        
        res.json(stakers.map(s => ({
            address: s.address,
            nftsStaked: s.nfts_staked,
            pondClaimed: s.address ? (claimedMap[s.address.toLowerCase()] || 0) : 0
        })));
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Leaderboard - Top Traders
app.get('/leaderboard/traders', (req, res) => {
    try {
        const limit = Math.min(parseInt(req.query.limit) || 25, 100);
        const traders = db.prepare(`
            SELECT 
                to_address as address,
                COUNT(*) as sales_count,
                SUM(CAST(price_sgb AS REAL)) as volume_sgb,
                SUM(CAST(price_pond AS REAL)) as volume_pond
            FROM events 
            WHERE event_type = 'sold'
            GROUP BY to_address
            ORDER BY volume_sgb DESC
            LIMIT ?
        `).all(limit);
        
        res.json(traders.map(t => ({
            address: t.address,
            salesCount: t.sales_count,
            volumeSGB: t.volume_sgb || 0,
            volumePOND: t.volume_pond || 0
        })));
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Leaderboard - Top LP Providers
app.get('/leaderboard/lp', (req, res) => {
    // LP leaderboard requires querying the pool contract for each depositor
    // For now, return empty - would need to index AddLiquidity events
    res.json([]);
});

// ==================== START ====================
app.listen(PORT, () => {
    console.log(`Toadz Indexer running on port ${PORT}`);
    
    // Initial sync
    indexEvents();
    updateFloors();
    
    // Start NFT ownership indexing (runs in background)
    setTimeout(() => indexNftOwnership(), 5000);
    
    // Poll every 15 seconds
    setInterval(indexEvents, POLL_INTERVAL);
    
    // Notify offer sellers every 30 seconds
    setInterval(notifyOfferSellers, 30000);
    
    // Update floors every 5 minutes
    setInterval(updateFloors, 300000);
    
    // Refresh ownership every 30 minutes
    setInterval(indexNftOwnership, 1800000);
});
