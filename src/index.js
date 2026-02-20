const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
require('dotenv').config();

const { connectDb, ensureSuperAdmin } = require('./db');
const authRoutes = require('./routes/auth');
const userRoutes = require('./routes/users');
const taskRoutes = require('./routes/tasks');
const eventRoutes = require('./routes/events');
const newsRoutes = require('./routes/news');
const discussionRoutes = require('./routes/discussions');
const analyticsRoutes = require('./routes/analytics');
const notificationRoutes = require('./routes/notifications');
const fileRoutes = require('./routes/files');
const attendanceRoutes = require('./routes/attendance');
const engagementRoutes = require('./routes/engagement');
const { startMonthlyAwardsScheduler, syncLegacyTaskGradePoints } = require('./services/engagement');

const app = express();
app.use(cors({ origin: '*' }));
app.use(express.json());

const uploadDir = path.join(__dirname, '..', 'uploads');
fs.mkdirSync(uploadDir, { recursive: true });
app.use('/uploads', express.static(uploadDir));

const certificateTemplateDir = path.join(__dirname, 'certifikat');
fs.mkdirSync(certificateTemplateDir, { recursive: true });
app.use('/certificates/templates', express.static(certificateTemplateDir));

app.get('/', (_req, res) => {
  res.json({ name: 'Talaba-Tarbiya API', status: 'ok' });
});

app.use('/api/auth', authRoutes);
app.use('/api/users', userRoutes);
app.use('/api/tasks', taskRoutes);
app.use('/api/events', eventRoutes);
app.use('/api/news', newsRoutes);
app.use('/api/discussions', discussionRoutes);
app.use('/api/analytics', analyticsRoutes);
app.use('/api/notifications', notificationRoutes);
app.use('/api/files', fileRoutes);
app.use('/api/attendance', attendanceRoutes);
app.use('/api/engagement', engagementRoutes);

const port = process.env.PORT || 4001;
connectDb()
  .then(() => ensureSuperAdmin())
  .then(() => syncLegacyTaskGradePoints().catch(() => ({ migrated: 0 })))
  .then(() => {
    startMonthlyAwardsScheduler();
    app.listen(port, () => {
      console.log(`API running on port ${port}`);
    });
  })
  .catch((error) => {
    console.error('Failed to start API', error);
  });
