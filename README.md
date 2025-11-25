# Toadz Indexer

Marketplace event indexer for ToadzStake.com

## Deploy to Railway

1. Create new project on Railway
2. Connect your GitHub repo (or deploy from this folder)
3. Railway auto-detects Node.js and deploys
4. Copy the generated URL (e.g., `https://toadz-indexer-production.up.railway.app`)
5. Update `INDEXER_URL` in your marketplace app.js

## API Endpoints

### Activity
- `GET /activity` - Recent activity (all collections)
- `GET /activity/:collection` - Activity for specific collection
- `GET /user/:address/activity` - User's activity

### Notifications
- `GET /user/:address/notifications` - All notifications
- `GET /user/:address/notifications/unread` - Unread counts + notifications
- `POST /user/:address/notifications/read` - Mark all as read

### Data
- `GET /floors` - Floor prices for all collections
- `GET /stats` - Volume, sales stats

## Notification Urgency

- **RED**: Money incoming (NFT sold, offer received)
- **GREEN**: User confirmations (listed, staked, purchased, offer accepted)

## Local Development

```bash
npm install
npm start
```

Runs on http://localhost:8080
