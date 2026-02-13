const express = require('express');
const bcrypt = require('bcryptjs');

const { requireAuth, requireRole } = require('../middleware/auth');
const { query, createId } = require('../db');
const { isId } = require('../utils/sql');

const router = express.Router();

router.get('/me', requireAuth, async (req, res) => {
  const userResult = await query(
    'SELECT id, role, email, avatar_url FROM users WHERE id = $1 LIMIT 1',
    [req.user.id],
  );
  if (userResult.rowCount === 0) {
    return res.status(404).json({ message: 'User topilmadi' });
  }

  const user = userResult.rows[0];
  const base = {
    id: user.id,
    role: user.role,
    email: user.email,
    avatar_url: user.avatar_url || null,
  };

  if (user.role === 'student') {
    const studentResult = await query(
      `SELECT full_name, curator_id, profile_json, spirituality_score, language, is_starosta
       FROM students WHERE user_id = $1 LIMIT 1`,
      [req.user.id],
    );
    if (studentResult.rowCount === 0) {
      return res.json({ ...base, student: null });
    }
    const student = studentResult.rows[0];
    return res.json({
      ...base,
      student: {
        full_name: student.full_name,
        curator_id: student.curator_id,
        profile_json: student.profile_json ?? null,
        spirituality_score: student.spirituality_score ?? 0,
        language: student.language ?? null,
        is_starosta: !!student.is_starosta,
      },
    });
  }

  if (user.role === 'admin' || user.role === 'super') {
    const adminResult = await query(
      'SELECT full_name, position, status FROM admins WHERE user_id = $1 LIMIT 1',
      [req.user.id],
    );
    if (adminResult.rowCount === 0) {
      return res.json({ ...base, admin: null });
    }
    const admin = adminResult.rows[0];
    return res.json({
      ...base,
      admin: {
        full_name: admin.full_name,
        position: admin.position,
        status: admin.status,
      },
    });
  }

  return res.json(base);
});

router.patch('/me', requireAuth, async (req, res) => {
  const { fullName, language, profileJson } = req.body || {};
  if (req.user.role === 'student') {
    await query(
      `UPDATE students
       SET full_name = COALESCE($2, full_name),
           language = COALESCE($3, language),
           profile_json = COALESCE($4::jsonb, profile_json)
       WHERE user_id = $1`,
      [req.user.id, fullName ?? null, language ?? null, profileJson ? JSON.stringify(profileJson) : null],
    );
  }
  return res.json({ message: 'Profil yangilandi' });
});

router.patch('/me/password', requireAuth, async (req, res) => {
  const { currentPassword, newPassword } = req.body || {};
  if (!currentPassword || !newPassword) {
    return res.status(400).json({ message: 'Parollar kerak' });
  }

  const userResult = await query('SELECT password_hash FROM users WHERE id = $1 LIMIT 1', [req.user.id]);
  if (userResult.rowCount === 0) {
    return res.status(404).json({ message: 'User topilmadi' });
  }

  const match = await bcrypt.compare(currentPassword, userResult.rows[0].password_hash);
  if (!match) {
    return res.status(400).json({ message: 'Joriy parol notogri' });
  }

  const nextHash = await bcrypt.hash(newPassword, 10);
  await query('UPDATE users SET password_hash = $2 WHERE id = $1', [req.user.id, nextHash]);

  return res.json({ message: 'Parol yangilandi' });
});

router.post('/me/avatar', requireAuth, async (req, res) => {
  const { avatarUrl } = req.body || {};
  if (!avatarUrl) {
    return res.status(400).json({ message: 'Avatar URL kerak' });
  }
  await query('UPDATE users SET avatar_url = $2 WHERE id = $1', [req.user.id, avatarUrl]);
  return res.json({ message: 'Avatar yangilandi', avatarUrl });
});

router.get('/admins', async (_req, res) => {
  const result = await query(
    `SELECT u.id, u.email, a.full_name, a.position, a.status
     FROM admins a
     JOIN users u ON u.id = a.user_id
     WHERE u.role = 'admin'
     ORDER BY a.full_name ASC`,
  );
  return res.json(
    result.rows.map((row) => ({
      id: row.id,
      email: row.email,
      full_name: row.full_name,
      position: row.position,
      status: row.status,
    })),
  );
});

router.get('/students', requireAuth, requireRole(['admin', 'super']), async (req, res) => {
  const params = [];
  let where = '';
  if (req.user.role === 'admin') {
    params.push(req.user.id);
    where = 'WHERE s.curator_id = $1';
  }

  const result = await query(
    `SELECT u.id, u.email, u.status, s.full_name, s.curator_id, s.is_starosta
     FROM students s
     JOIN users u ON u.id = s.user_id
     ${where}
     ORDER BY s.full_name ASC`,
    params,
  );

  return res.json(
    result.rows.map((row) => ({
      id: row.id,
      email: row.email,
      full_name: row.full_name,
      curator_id: row.curator_id,
      status: row.status || 'active',
      is_starosta: !!row.is_starosta,
    })),
  );
});

router.post('/students', requireAuth, requireRole(['admin', 'super']), async (req, res) => {
  const { email, password, fullName, curatorId } = req.body || {};
  if (!email || !password || !fullName) {
    return res.status(400).json({ message: 'Maydonlar toliq emas' });
  }

  const normalizedEmail = email.trim().toLowerCase();
  const existing = await query('SELECT id FROM users WHERE email = $1 LIMIT 1', [normalizedEmail]);
  if (existing.rowCount > 0) {
    return res.status(409).json({ message: 'Email mavjud' });
  }

  const targetCuratorId = curatorId || req.user.id;
  if (!isId(targetCuratorId)) {
    return res.status(400).json({ message: 'Kurator topilmadi' });
  }

  const adminCheck = await query(
    `SELECT a.user_id
     FROM admins a
     JOIN users u ON u.id = a.user_id
     WHERE a.user_id = $1 AND u.role = 'admin'
     LIMIT 1`,
    [targetCuratorId],
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
    [userId, targetCuratorId, fullName],
  );

  return res.status(201).json({ message: 'Student yaratildi' });
});

router.delete('/students/:id', requireAuth, requireRole(['admin', 'super']), async (req, res) => {
  const { password } = req.body || {};
  const targetId = req.params.id;

  if (!password) {
    return res.status(400).json({ message: 'Parol kerak' });
  }
  if (!isId(targetId)) {
    return res.status(400).json({ message: 'Student topilmadi' });
  }

  const adminUser = await query('SELECT password_hash FROM users WHERE id = $1 LIMIT 1', [req.user.id]);
  if (adminUser.rowCount === 0) {
    return res.status(404).json({ message: 'Admin topilmadi' });
  }
  const match = await bcrypt.compare(password, adminUser.rows[0].password_hash);
  if (!match) {
    return res.status(401).json({ message: 'Parol notogri' });
  }

  const student = await query('SELECT curator_id FROM students WHERE user_id = $1 LIMIT 1', [targetId]);
  if (student.rowCount === 0) {
    return res.status(404).json({ message: 'Student topilmadi' });
  }
  if (req.user.role === 'admin' && student.rows[0].curator_id !== req.user.id) {
    return res.status(403).json({ message: 'Ruxsat yoq' });
  }

  await query('DELETE FROM users WHERE id = $1', [targetId]);
  return res.json({ message: 'Student ochirildi' });
});

router.patch('/students/:id/starosta', requireAuth, requireRole(['admin', 'super']), async (req, res) => {
  const { isStarosta } = req.body || {};
  const targetId = req.params.id;

  if (!isId(targetId)) {
    return res.status(400).json({ message: 'Student topilmadi' });
  }

  const studentRes = await query(
    'SELECT user_id, curator_id, is_starosta FROM students WHERE user_id = $1 LIMIT 1',
    [targetId],
  );
  if (studentRes.rowCount === 0) {
    return res.status(404).json({ message: 'Student topilmadi' });
  }
  const student = studentRes.rows[0];

  if (req.user.role === 'admin' && student.curator_id !== req.user.id) {
    return res.status(403).json({ message: 'Ruxsat yoq' });
  }

  const nextValue = !!isStarosta;
  const countRes = await query(
    `SELECT COUNT(*)::int AS count
     FROM students
     WHERE curator_id = $1 AND is_starosta = TRUE AND user_id <> $2`,
    [student.curator_id, student.user_id],
  );
  const currentCount = countRes.rows[0].count;

  if (nextValue && currentCount >= 3) {
    return res.status(400).json({ message: 'Starosta limiti 3 ta' });
  }
  if (!nextValue && currentCount < 1) {
    return res.status(400).json({ message: 'Kamida bitta starosta kerak' });
  }

  await query(
    `UPDATE students
     SET is_starosta = $2,
         starosta_assigned_at = $3,
         starosta_assigned_by = $4
     WHERE user_id = $1`,
    [targetId, nextValue, nextValue ? new Date() : null, nextValue ? req.user.id : null],
  );

  return res.json({ message: 'Starosta yangilandi', is_starosta: nextValue });
});

router.post('/admins', requireAuth, requireRole(['super']), async (req, res) => {
  const { email, password, fullName } = req.body || {};
  if (!email || !password || !fullName) {
    return res.status(400).json({ message: 'Maydonlar toliq emas' });
  }

  const normalizedEmail = email.trim().toLowerCase();
  const existing = await query('SELECT id FROM users WHERE email = $1 LIMIT 1', [normalizedEmail]);
  if (existing.rowCount > 0) {
    return res.status(409).json({ message: 'Email mavjud' });
  }

  const userId = createId();
  const passwordHash = await bcrypt.hash(password, 10);
  await query(
    `INSERT INTO users (id, role, email, password_hash)
     VALUES ($1, $2, $3, $4)`,
    [userId, 'admin', normalizedEmail, passwordHash],
  );
  await query(
    `INSERT INTO admins (user_id, full_name, position, status)
     VALUES ($1, $2, $3, $4)`,
    [userId, fullName, 'Curator', 'active'],
  );

  return res.status(201).json({ message: 'Admin yaratildi' });
});

router.patch('/admins/:id', requireAuth, requireRole(['super']), async (req, res) => {
  const { fullName, status } = req.body || {};
  if (!isId(req.params.id)) {
    return res.status(400).json({ message: 'Admin topilmadi' });
  }

  await query(
    `UPDATE admins
     SET full_name = COALESCE($2, full_name),
         status = COALESCE($3, status)
     WHERE user_id = $1`,
    [req.params.id, fullName ?? null, status ?? null],
  );

  return res.json({ message: 'Admin yangilandi' });
});

router.delete('/admins/:id', requireAuth, requireRole(['super']), async (req, res) => {
  if (!isId(req.params.id)) {
    return res.status(400).json({ message: 'Admin topilmadi' });
  }
  await query('DELETE FROM users WHERE id = $1', [req.params.id]);
  return res.json({ message: 'Admin ochirildi' });
});

module.exports = router;
