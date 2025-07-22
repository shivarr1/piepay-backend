// server.js
// A simple backend service for the PiePay take-home assignment.
// Author: Gemini
// Version: MySQL Integration

const express = require('express');
const mysql = require('mysql2/promise');
require('dotenv').config(); // To load environment variables from a .env file

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware to parse JSON bodies
app.use(express.json());

// --- MySQL Database Connection ---
// Create a connection pool to efficiently manage connections to the MySQL database.
// Connection details are stored in a .env file for security.
const dbPool = mysql.createPool({
    host: process.env.DB_HOST || 'localhost',
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

// --- Helper Functions ---

/**
 * Creates a unique ID for an offer based on its description to prevent duplicates.
 * @param {string} description - The offer description.
 * @returns {string} A hash representing the offer.
 */
const createOfferId = (description) => {
    // A simple approach to create a consistent ID for duplicate checking.
    return description.replace(/\s+/g, '').toLowerCase();
};

/**
 * Parses the assumed structure of Flipkart's API response to extract offers.
 * @param {object} flipkartApiResponse - The mock response from Flipkart's offer API.
 * @returns {Array} An array of standardized offer objects.
 */
const parseOffersFromPayload = (flipkartApiResponse) => {
    if (!flipkartApiResponse || !Array.isArray(flipkartApiResponse.offers)) {
        console.error("Invalid or missing offers array in the payload.");
        return [];
    }
    return flipkartApiResponse.offers.map(rawOffer => ({
        id: createOfferId(rawOffer.description),
        description: rawOffer.description,
        bankName: rawOffer.bankName,
        paymentInstrument: rawOffer.paymentInstrument,
        discountType: rawOffer.discountType,
        discountValue: rawOffer.discountValue,
        maxDiscount: rawOffer.maxDiscount || null,
        minTxnValue: rawOffer.minTxnValue,
    }));
};


// --- API Endpoints ---

/**
 * Endpoint to receive Flipkart's offer API response, parse offers, and store them in MySQL.
 * Handles duplicates by using "INSERT IGNORE" which relies on a UNIQUE key in the DB table.
 *
 * @route POST /offer
 * @param {object} req.body - The request body containing the flipkartOfferApiResponse.
 * @returns {object} A JSON object with counts of identified and newly created offers.
 */
app.post('/offer', async (req, res) => {
    try {
        const { flipkartOfferApiResponse } = req.body;
        if (!flipkartApiResponse) {
            return res.status(400).json({ error: 'Missing flipkartOfferApiResponse in request body.' });
        }

        const identifiedOffers = parseOffersFromPayload(flipkartApiResponse);
        if (identifiedOffers.length === 0) {
            return res.status(200).json({
                noOfOffersIdentified: 0,
                noOfNewOffersCreated: 0,
            });
        }

        const sql = `
            INSERT IGNORE INTO offers (id, description, bankName, paymentInstrument, discountType, discountValue, maxDiscount, minTxnValue)
            VALUES ?
        `;

        // Map array of objects to array of arrays for the query
        const values = identifiedOffers.map(o => [o.id, o.description, o.bankName, o.paymentInstrument, o.discountType, o.discountValue, o.maxDiscount, o.minTxnValue]);

        const [result] = await dbPool.query(sql, [values]);

        res.status(201).json({
            noOfOffersIdentified: identifiedOffers.length,
            noOfNewOffersCreated: result.affectedRows,
        });

    } catch (error) {
        console.error('Error in /offer endpoint:', error);
        res.status(500).json({ error: 'An internal server error occurred while saving offers.' });
    }
});

/**
 * Endpoint to find the highest applicable discount for a given payment scenario from MySQL.
 *
 * @route GET /highest-discount
 * @query {number} amountToPay - The total amount of the transaction.
 * @query {string} bankName - The name of the bank (e.g., "AXIS").
 * @query {string} paymentInstrument - (Bonus) The payment method (e.g., "CREDIT").
 * @returns {object} A JSON object with the highest calculated discount amount.
 */
app.get('/highest-discount', async (req, res) => {
    try {
        const { amountToPay, bankName, paymentInstrument } = req.query;

        if (!amountToPay || !bankName || !paymentInstrument) {
            return res.status(400).json({
                error: 'Missing required query parameters: amountToPay, bankName, and paymentInstrument are required.'
            });
        }

        const parsedAmount = parseFloat(amountToPay);
        if (isNaN(parsedAmount)) {
            return res.status(400).json({ error: 'Invalid amountToPay. Must be a number.' });
        }

        // 1. Query the database to fetch applicable offers
        const sql = `
            SELECT * FROM offers
            WHERE bankName = ?
            AND paymentInstrument = ?
            AND minTxnValue <= ?
        `;

        const [applicableOffers] = await dbPool.query(sql, [bankName.toUpperCase(), paymentInstrument.toUpperCase(), parsedAmount]);

        if (applicableOffers.length === 0) {
            return res.json({ highestDiscountAmount: 0 });
        }

        // 2. Calculate the discount for each applicable offer
        let highestDiscount = 0;
        applicableOffers.forEach(offer => {
            let currentDiscount = 0;
            if (offer.discountType === 'FLAT') {
                currentDiscount = offer.discountValue;
            } else if (offer.discountType === 'PERCENTAGE') {
                currentDiscount = (offer.discountValue / 100) * parsedAmount;
                if (offer.maxDiscount && currentDiscount > offer.maxDiscount) {
                    currentDiscount = offer.maxDiscount;
                }
            }

            // 3. Keep track of the highest discount found
            if (currentDiscount > highestDiscount) {
                highestDiscount = currentDiscount;
            }
        });

        res.json({
            highestDiscountAmount: parseFloat(highestDiscount.toFixed(2))
        });

    } catch (error) {
        console.error('Error in /highest-discount endpoint:', error);
        res.status(500).json({ error: 'An internal server error occurred while calculating discount.' });
    }
});

// A simple root endpoint to confirm the server is running.
app.get('/', (req, res) => {
    res.send('PiePay Assignment Backend Service (MySQL Version) is running!');
});

// Graceful shutdown
process.on('SIGINT', async () => {
    await dbPool.end();
    console.log('MySQL connection pool closed.');
    process.exit(0);
});

app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});
