// server.js - Main entry point
const express = require('express');
const app = express();
const port = process.env.PORT || 3003;
const itemRoutes = require('./routes/minioRoutes');

// Middleware
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static('public'));

// Routes
app.get('/', (req, res) => {
  res.send('Hello World! Welcome to Express.js server');
});

// Use item routes
app.use('/api/meters', itemRoutes);

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).send('Something broke!');
});

// Start the server
app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});
