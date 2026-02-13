const express = require('express');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');

const { sendOtp } = require('../services/email');
const { query, createId } = require('../db');
const { isId } = require('../utils/sql');

const router = express.Router();

function generateOtp() {
  return Math.floor(100000 + Math.random() * 900000).toString();
}

router.post('/otp/send', async (req, res) => {
  const { email } = req.body || {};
  if (!email) {
    return res.status(400).json({ message: 'Email kerak' });
  }

  const normalizedEmail = email.trim().toLowerCase();
  const code = generateOtp();
  const expiresAt = new Date(Date.now() + 10 * 60 * 1000);

  await query(
    `INSERT INTO otp_verifications (id, email, code, expires_at)
     VALUES ($1, $2, $3, $4)`,
    [createId(), normalizedEmail, code, expiresAt],
  );

  try {
    await sendOtp(normalizedEmail, code);
    return res.json({ message: 'OTP yuborildi' });
  } catch (_error) {
    return res.status(500).json({ message: 'Email yuborishda xatolik' });
  }
});

router.post('/otp/verify', async (req, res) => {
  const { email, code } = req.body || {};
  if (!email || !code) {
    return res.status(400).json({ message: 'Email va kod kerak' });
  }

  const normalizedEmail = email.trim().toLowerCase();
  const record = await query(
    `SELECT id, expires_at
     FROM otp_verifications
     WHERE email = $1 AND code = $2
     ORDER BY created_at DESC
     LIMIT 1`,
    [normalizedEmail, code],
  );

  if (record.rowCount === 0) {
    return res.status(400).json({ message: 'OTP notogri' });
  }

  const otp = record.rows[0];
  if (new Date(otp.expires_at) < new Date()) {
    return res.status(400).json({ message: 'OTP muddati tugagan' });
  }

  await query('UPDATE otp_verifications SET verified_at = NOW() WHERE id = $1', [otp.id]);
  return res.json({ message: 'OTP tasdiqlandi' });
});

router.post('/register', async (req, res) => {
  const { email, password, fullName, curatorId } = req.body || {};
  if (!email || !password || !fullName || !curatorId) {
    return res.status(400).json({ message: 'Maydonlar toliq emas' });
  }

  if (!isId(curatorId)) {
    return res.status(400).json({ message: 'Kurator topilmadi' });
  }

  const normalizedEmail = email.trim().toLowerCase();
  const verified = await query(
    `SELECT id
     FROM otp_verifications
     WHERE email = $1 AND verified_at IS NOT NULL
     ORDER BY created_at DESC
     LIMIT 1`,
    [normalizedEmail],
  );

  if (verified.rowCount === 0) {
    return res.status(400).json({ message: 'OTP tasdiqlanmagan' });
  }

  const existing = await query('SELECT id FROM users WHERE email = $1 LIMIT 1', [normalizedEmail]);
  if (existing.rowCount > 0) {
    return res.status(409).json({ message: 'Email mavjud' });
  }

  const adminCheck = await query(
    `SELECT a.user_id
     FROM admins a
     JOIN users u ON u.id = a.user_id
     WHERE a.user_id = $1 AND u.role = 'admin'
     LIMIT 1`,
    [curatorId],
  );
  if (adminCheck.rowCount === 0) {
    return res.status(400).json({ message: 'Kurator topilmadi' });
  }

  const userId = createId();
  const passwordHash = await bcrypt.hash(password, 10);
  await query(
    `INSERT INTO users (id, role, email, password_hash)
     VALUES ($1, $2, $3, $4)`,
    [userId, 'student', normalizedEmail, passwordHash],
  );

  await query(
    `INSERT INTO students (user_id, curator_id, full_name)
     VALUES ($1, $2, $3)`,
    [userId, curatorId, fullName],
  );

  return res.status(201).json({ message: 'Student royxatdan otdi' });
});

router.post('/login', async (req, res) => {
  const { email, password } = req.body || {};
  if (!email || !password) {
    return res.status(400).json({ message: 'Email va parol kerak' });
  }

  const normalizedEmail = email.trim().toLowerCase();
  const result = await query(
    'SELECT id, role, password_hash FROM users WHERE email = $1 LIMIT 1',
    [normalizedEmail],
  );
  if (result.rowCount === 0) {
    return res.status(401).json({ message: 'Login yoki parol notogri' });
  }

  const user = result.rows[0];
  const match = await bcrypt.compare(password, user.password_hash);
  if (!match) {
    return res.status(401).json({ message: 'Login yoki parol notogri' });
  }

  const token = jwt.sign(
    { id: user.id, role: user.role },
    process.env.JWT_SECRET,
    { expiresIn: '7d' },
  );

  return res.json({ token, role: user.role });
});

module.exports = router;
