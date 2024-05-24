const express = require('express');
const multer = require('multer');
const csv = require('csv-parser');
const mongoose = require('mongoose');
const fs = require('fs');

// Connect to MongoDB
mongoose.connect('mongodb+srv://kpushpankar3:pushpa123@cluster0.jxymuov.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0/trades', {
  useNewUrlParser: true,
  useUnifiedTopology: true
});
const db = mongoose.connection;
db.on('error', console.error.bind(console, 'MongoDB connection error:'));
db.once('open', () => {
  console.log('Connected to MongoDB');
});

// Define the trade schema
const tradeSchema = new mongoose.Schema({
    User_ID:Number,
    UTC_Time: String,
    Operation: String,
    Market: String,
    Buy_Sell_Amount: Number,
    Price: Number,
  });

// Create the trade model
const Trade = mongoose.model('Trade', tradeSchema);

// Create an Express app
const app = express();
app.use(express.json());
// Set up multer for file uploads
const upload = multer({ dest: 'uploads/' });

// Define the API endpoint for uploading the CSV file
app.post('/upload', upload.single('csv'), async (req, res) => {
    try {
      // Parse the CSV file
      const results = [];
      fs.createReadStream(req.file.path)
        .pipe(csv())
        .on('data', (data) => results.push(data))
        .on('end', async () => {
          // Store the trade data in the database
          await Trade.insertMany(results);
          res.send('Trade data stored successfully');
        });
    } catch (err) {
      console.error(err);
      res.status(500).send('Error storing trade data');
    }
  });
  app.post('/balance', async (req, res) => {
    try {
      const { timestamp } = req.body;
  
      // Ensure the timestamp is in the correct format for querying
      const formattedTimestamp = new Date(timestamp);
      console.log(`Formatted Timestamp: ${formattedTimestamp}`);
  
      // Find all trades before the given timestamp
      const trades = await Trade.find({ UTC_Time: { $lt: formattedTimestamp } });
      console.log(`Trades before ${formattedTimestamp}:`, trades);
  
      // Initialize balance object to store asset-wise balance
      const balance = {};
  
      // Iterate through trades to calculate balance
      trades.forEach((trade) => {
        const { Operation, Market, Buy_Sell_Amount } = trade;
        const [baseCoin] = Market.split('/');
        const amount = parseFloat(Buy_Sell_Amount);
  
        if (Operation === 'Buy') {
          balance[baseCoin] = (balance[baseCoin] || 0) + amount;
        } else if (Operation === 'Sell') {
          balance[baseCoin] = (balance[baseCoin] || 0) - amount;
        }
      });
  
      console.log('Calculated Balance:', balance);
  
      // Filter out coins with zero balance
      const filteredBalance = {};
      Object.keys(balance).forEach((coin) => {
        if (balance[coin] !== 0) {
          filteredBalance[coin] = balance[coin];
        }
      });
  
      console.log('Filtered Balance:', filteredBalance);
  
      res.json(filteredBalance);
    } catch (err) {
      console.error(err);
      res.status(500).send('Error calculating balance');
    }
  });
  
  

// Start the server
const port = 3000;
app.listen(port, () => {
  console.log(`Server listening on port ${port}`);
});
