const express = require('express');
const mysql = require('mysql2/promise');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

// MySQL connection pool
const dbPool = mysql.createPool({
    host: process.env.DB_HOST || 'localhost',
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

// Create a unique ID for an offer based on its description
const createOfferId = (description) => {
    return description.replace(/\s+/g, '').toLowerCase();
};

// Parse offers from Flipkart API response
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

// Store offers in MySQL, ignoring duplicates
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

// Find the highest applicable discount for a payment scenario
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

// Root endpoint
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
