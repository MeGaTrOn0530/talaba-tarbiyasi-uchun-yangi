const express = require('express');

const { createId, query } = require('../db');
const { requireAuth, requireRole } = require('../middleware/auth');
const { isId } = require('../utils/sql');
const {
  addNotification,
  applyPoints,
  awardBadge,
  getLeaderboard,
  getStudentProgress,
  getStudentRank,
  levelInfo,
  normalizeLimit,
  resolveMonthlyCertificateTemplates,
  runMonthlyAwardsIfDue,
  syncStudentGamification,
  toInt,
} = require('../services/engagement');

const router = express.Router();

function hasOwn(obj, key) {
  return !!obj && Object.prototype.hasOwnProperty.call(obj, key);
}

function toDate(raw) {
  if (!raw) return null;
  const parsed = raw instanceof Date ? raw : new Date(raw);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed;
}

function resolveDate(raw, fallback = null) {
  const parsed = toDate(raw);
  return parsed || fallback;
}

function inArray(value, list) {
  return list.includes(String(value || '').toLowerCase());
}

async function ensureMonthlyAwardsBackground() {
  try {
    await runMonthlyAwardsIfDue();
  } catch (_) {}
}

async function fetchStudentRow(studentId) {
  const result = await query(
    `SELECT s.user_id, s.full_name, s.curator_id, s.spirituality_score
     FROM students s
     WHERE s.user_id = $1
     LIMIT 1`,
    [studentId],
  );
  return result.rowCount > 0 ? result.rows[0] : null;
}

async function ensureStudentAccess(req, res, studentId) {
  const student = await fetchStudentRow(studentId);
  if (!student) {
    res.status(404).json({ message: 'Talaba topilmadi' });
    return null;
  }
  if (req.user.role === 'admin' && student.curator_id !== req.user.id) {
    res.status(403).json({ message: 'Ruxsat yoq' });
    return null;
  }
  return student;
}

async function resolveStudentIdByRole(req, res, requestedStudentId = null) {
  if (req.user.role === 'student') {
    return req.user.id;
  }

  if (!requestedStudentId) {
    res.status(400).json({ message: 'studentId kerak' });
    return null;
  }
  if (!isId(requestedStudentId)) {
    res.status(400).json({ message: 'Notogri ID' });
    return null;
  }
  const student = await ensureStudentAccess(req, res, requestedStudentId);
  if (!student) return null;
  return student.user_id;
}

async function fetchPendingTaskStats(studentId) {
  const result = await query(
    `SELECT t.id,
            t.title,
            t.deadline_at,
            ta.id AS assignment_id,
            ta.status AS assignment_status
     FROM students s
     INNER JOIN tasks t ON t.curator_id = s.curator_id
     LEFT JOIN task_assignments ta
            ON ta.task_id = t.id
           AND ta.student_id = s.user_id
     WHERE s.user_id = $1
       AND (t.status IS NULL OR t.status = 'active')
     ORDER BY t.deadline_at ASC, t.created_at DESC`,
    [studentId],
  );

  const now = new Date();
  const dueSoon = new Date(now.getTime() + 1000 * 60 * 60 * 72);
  let pendingCount = 0;
  let dueSoonCount = 0;
  const dueSoonTitles = [];

  for (const row of result.rows) {
    const status = String(row.assignment_status || '').toLowerCase();
    const submitted =
      !!row.assignment_id &&
      ['under_review', 'graded', 'rejected', 'approved', 'submitted'].includes(status);

    if (!submitted) {
      pendingCount += 1;
      const deadline = toDate(row.deadline_at);
      if (deadline && deadline >= now && deadline <= dueSoon) {
        dueSoonCount += 1;
        if (dueSoonTitles.length < 5) {
          dueSoonTitles.push(row.title || 'Vazifa');
        }
      }
    }
  }

  const reminders = [];
  if (pendingCount > 0) {
    reminders.push(`Sizga ${pendingCount} ta vazifa qoldi`);
  }
  if (dueSoonCount > 0) {
    reminders.push(`${dueSoonCount} ta vazifa muddati 72 soatda tugaydi`);
  }
  if (dueSoonTitles.length > 0) {
    reminders.push(`Yaqin muddat: ${dueSoonTitles.join(', ')}`);
  }

  return {
    pending_count: pendingCount,
    due_soon_count: dueSoonCount,
    reminders,
  };
}

function challengeRankBonus(bonusPoints, rankPosition) {
  const bonus = Math.max(0, toInt(bonusPoints, 0));
  const rank = toInt(rankPosition, 0);
  if (bonus < 1 || rank < 1) return 0;
  if (rank === 1) return bonus;
  if (rank === 2) return Math.max(0, Math.round(bonus * 0.7));
  if (rank === 3) return Math.max(0, Math.round(bonus * 0.4));
  return 0;
}

async function maybeCreateRankCertificate({
  studentId,
  challengeId,
  rankPosition,
  issuedBy,
}) {
  const rank = toInt(rankPosition, 0);
  if (![1, 2, 3].includes(rank)) return;

  const existing = await query(
    `SELECT id
     FROM certificates
     WHERE student_id = $1
       AND challenge_id = $2
       AND rank_label = $3
     LIMIT 1`,
    [studentId, challengeId, `${rank}-o'rin`],
  );
  if (existing.rowCount > 0) return;

  await query(
    `INSERT INTO certificates (
      id, student_id, challenge_id, issued_by, title, rank_label, note, template_name
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
    [
      createId(),
      studentId,
      challengeId,
      issuedBy,
      'Haftalik challenge g\'olibi',
      `${rank}-o'rin`,
      'Avtomatik yaratilgan sertifikat. PDF shablon keyin ulanadi.',
      'weekly_challenge_default',
    ],
  );
}

async function fetchChallengeRow(challengeId) {
  const result = await query(
    `SELECT *
     FROM weekly_challenges
     WHERE id = $1
     LIMIT 1`,
    [challengeId],
  );
  return result.rowCount > 0 ? result.rows[0] : null;
}

router.get('/me', requireAuth, async (req, res) => {
  await ensureMonthlyAwardsBackground();
  const requestedStudentId = req.query.studentId ? String(req.query.studentId) : null;
  const studentId = await resolveStudentIdByRole(req, res, requestedStudentId);
  if (!studentId) return;

  const student = await ensureStudentAccess(req, res, studentId);
  if (!student) return;

  const [snapshot, rank, globalLeaderboard, progress, reminderStats, certCountRes] = await Promise.all([
    syncStudentGamification(studentId),
    getStudentRank(studentId),
    getLeaderboard({ limit: 10 }),
    getStudentProgress(studentId, 6),
    fetchPendingTaskStats(studentId),
    query(
      `SELECT COUNT(*)::int AS count
       FROM certificates
       WHERE student_id = $1`,
      [studentId],
    ),
  ]);

  return res.json({
    student_id: studentId,
    full_name: student.full_name || 'Talaba',
    score: snapshot.score,
    level: snapshot.level,
    weekly_streak: snapshot.streak,
    badges: snapshot.badges,
    rank: rank,
    leaderboard_top10: globalLeaderboard,
    progress,
    reminders: reminderStats,
    certificates_count: toInt(certCountRes.rows[0]?.count, 0),
  });
});

router.get('/leaderboard', requireAuth, async (req, res) => {
  await ensureMonthlyAwardsBackground();
  const includeAll =
    String(req.query.all || '').toLowerCase() === '1' ||
    String(req.query.all || '').toLowerCase() === 'true';
  const limit = normalizeLimit(req.query.limit, 1, 100);
  const scope = String(req.query.scope || 'global').toLowerCase();
  let curatorId = null;

  if (scope === 'curator') {
    if (req.user.role === 'admin') {
      curatorId = req.user.id;
    } else if (req.user.role === 'student') {
      const student = await fetchStudentRow(req.user.id);
      curatorId = student?.curator_id || null;
    } else if (req.user.role === 'super' && req.query.curatorId && isId(String(req.query.curatorId))) {
      curatorId = String(req.query.curatorId);
    }
  }

  const rows = await getLeaderboard({ limit, curatorId, includeAll });
  return res.json({
    scope: curatorId ? 'curator' : 'global',
    items: rows,
  });
});

router.get('/certificate-templates', requireAuth, requireRole(['super']), async (req, res) => {
  const templates = resolveMonthlyCertificateTemplates(true);
  return res.json({
    first: templates?.first || null,
    second: templates?.second || null,
    third: templates?.third || null,
    top: templates?.top || null,
  });
});

router.get('/challenges', requireAuth, async (req, res) => {
  const status = req.query.status ? String(req.query.status).toLowerCase() : null;
  const params = [];
  let where = '';
  if (status) {
    params.push(status);
    where = 'WHERE LOWER(wc.status) = $1';
  }

  const result = await query(
    `SELECT wc.*,
            (SELECT COUNT(*)::int FROM weekly_challenge_entries wce WHERE wce.challenge_id = wc.id) AS entry_count,
            (SELECT COUNT(*)::int FROM weekly_challenge_entries wce WHERE wce.challenge_id = wc.id AND wce.status IN ('approved', 'graded')) AS approved_count
     FROM weekly_challenges wc
     ${where}
     ORDER BY wc.starts_at DESC, wc.created_at DESC`,
    params,
  );

  let myEntryMap = new Map();
  if (req.user.role === 'student') {
    const entryRes = await query(
      `SELECT *
       FROM weekly_challenge_entries
       WHERE student_id = $1`,
      [req.user.id],
    );
    myEntryMap = new Map(entryRes.rows.map((row) => [row.challenge_id, row]));
  }

  const now = new Date();
  const items = result.rows.map((row) => {
    const startsAt = toDate(row.starts_at);
    const endsAt = toDate(row.ends_at);
    const isActive =
      String(row.status || '').toLowerCase() === 'active' &&
      !!startsAt &&
      !!endsAt &&
      startsAt <= now &&
      endsAt >= now;
    const mine = myEntryMap.get(row.id) || null;
    return {
      ...row,
      entry_count: toInt(row.entry_count, 0),
      approved_count: toInt(row.approved_count, 0),
      is_active: isActive,
      my_entry: mine
        ? {
            id: mine.id,
            status: mine.status,
            score: mine.score,
            rank_position: mine.rank_position,
            updated_at: mine.updated_at,
            file_url: mine.file_url,
            text: mine.text,
          }
        : null,
    };
  });

  return res.json(items);
});

router.post('/challenges', requireAuth, requireRole(['admin', 'super']), async (req, res) => {
  const {
    title,
    description,
    category,
    startsAt,
    endsAt,
    bonusPoints,
    rewardText,
    mode,
  } = req.body || {};

  if (!title || !startsAt || !endsAt) {
    return res.status(400).json({ message: 'Sarlavha va muddatlar kerak' });
  }

  const starts = resolveDate(startsAt);
  const ends = resolveDate(endsAt);
  if (!starts || !ends || starts >= ends) {
    return res.status(400).json({ message: 'Muddat notogri' });
  }

  const normalizedMode = String(mode || 'solo').toLowerCase();
  if (!['solo', 'group', 'duel'].includes(normalizedMode)) {
    return res.status(400).json({ message: 'Mode notogri' });
  }

  const challengeId = createId();
  await query(
    `INSERT INTO weekly_challenges (
      id, created_by, title, description, category, mode, reward_text, bonus_points, starts_at, ends_at, status
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
    [
      challengeId,
      req.user.id,
      String(title).trim(),
      description ? String(description).trim() : null,
      category ? String(category).trim() : null,
      normalizedMode,
      rewardText ? String(rewardText).trim() : null,
      Math.max(0, toInt(bonusPoints, 0)),
      starts,
      ends,
      'active',
    ],
  );

  return res.status(201).json({ id: challengeId, message: 'Haftalik challenge yaratildi' });
});

router.patch('/challenges/:id', requireAuth, requireRole(['admin', 'super']), async (req, res) => {
  if (!isId(req.params.id)) {
    return res.status(400).json({ message: 'Notogri ID' });
  }
  const challenge = await fetchChallengeRow(req.params.id);
  if (!challenge) {
    return res.status(404).json({ message: 'Challenge topilmadi' });
  }
  if (req.user.role === 'admin' && challenge.created_by !== req.user.id) {
    return res.status(403).json({ message: 'Ruxsat yoq' });
  }

  const updates = req.body || {};
  const allowedStatuses = ['draft', 'active', 'completed', 'archived'];
  if (hasOwn(updates, 'status')) {
    const nextStatus = String(updates.status || '').toLowerCase();
    if (!allowedStatuses.includes(nextStatus)) {
      return res.status(400).json({ message: 'Status notogri' });
    }
  }

  await query(
    `UPDATE weekly_challenges
     SET title = COALESCE($2, title),
         description = COALESCE($3, description),
         category = COALESCE($4, category),
         mode = COALESCE($5, mode),
         reward_text = COALESCE($6, reward_text),
         bonus_points = COALESCE($7, bonus_points),
         starts_at = COALESCE($8, starts_at),
         ends_at = COALESCE($9, ends_at),
         status = COALESCE($10, status)
     WHERE id = $1`,
    [
      req.params.id,
      updates.title ? String(updates.title).trim() : null,
      hasOwn(updates, 'description') ? (updates.description ? String(updates.description).trim() : null) : null,
      updates.category ? String(updates.category).trim() : null,
      updates.mode ? String(updates.mode).toLowerCase() : null,
      hasOwn(updates, 'rewardText') ? (updates.rewardText ? String(updates.rewardText).trim() : null) : null,
      hasOwn(updates, 'bonusPoints') ? Math.max(0, toInt(updates.bonusPoints, 0)) : null,
      hasOwn(updates, 'startsAt') ? resolveDate(updates.startsAt, null) : null,
      hasOwn(updates, 'endsAt') ? resolveDate(updates.endsAt, null) : null,
      hasOwn(updates, 'status') ? String(updates.status).toLowerCase() : null,
    ],
  );

  return res.json({ message: 'Challenge yangilandi' });
});

router.delete('/challenges/:id', requireAuth, requireRole(['admin', 'super']), async (req, res) => {
  if (!isId(req.params.id)) {
    return res.status(400).json({ message: 'Notogri ID' });
  }

  const challenge = await fetchChallengeRow(req.params.id);
  if (!challenge) {
    return res.status(404).json({ message: 'Challenge topilmadi' });
  }
  if (req.user.role === 'admin' && challenge.created_by !== req.user.id) {
    return res.status(403).json({ message: 'Ruxsat yoq' });
  }

  await query('DELETE FROM weekly_challenges WHERE id = $1', [req.params.id]);
  return res.json({ message: 'Challenge ochirildi' });
});

router.get('/challenges/:id/entries', requireAuth, async (req, res) => {
  if (!isId(req.params.id)) {
    return res.status(400).json({ message: 'Notogri ID' });
  }
  const challenge = await fetchChallengeRow(req.params.id);
  if (!challenge) {
    return res.status(404).json({ message: 'Challenge topilmadi' });
  }

  const entriesRes = await query(
    `SELECT wce.*,
            s.full_name AS student_name,
            (SELECT COUNT(*)::int FROM challenge_entry_likes cel WHERE cel.entry_id = wce.id) AS likes_count,
            (SELECT COUNT(*)::int FROM challenge_entry_comments cec WHERE cec.entry_id = wce.id) AS comments_count,
            (SELECT COUNT(*)::int FROM challenge_entry_likes cel2 WHERE cel2.entry_id = wce.id AND cel2.user_id = $2) AS liked_by_me
     FROM weekly_challenge_entries wce
     INNER JOIN students s ON s.user_id = wce.student_id
     WHERE wce.challenge_id = $1
     ORDER BY COALESCE(wce.rank_position, 9999), wce.updated_at DESC`,
    [req.params.id, req.user.id],
  );

  return res.json(
    entriesRes.rows.map((row) => ({
      ...row,
      likes_count: toInt(row.likes_count, 0),
      comments_count: toInt(row.comments_count, 0),
      liked_by_me: toInt(row.liked_by_me, 0) > 0,
    })),
  );
});

router.post('/challenges/:id/entries', requireAuth, requireRole(['student']), async (req, res) => {
  if (!isId(req.params.id)) {
    return res.status(400).json({ message: 'Notogri ID' });
  }
  const challenge = await fetchChallengeRow(req.params.id);
  if (!challenge) {
    return res.status(404).json({ message: 'Challenge topilmadi' });
  }

  const now = new Date();
  const starts = toDate(challenge.starts_at);
  const ends = toDate(challenge.ends_at);
  if (String(challenge.status).toLowerCase() !== 'active' || !starts || !ends || now < starts || now > ends) {
    return res.status(400).json({ message: 'Challenge aktiv emas' });
  }

  const { text, fileUrl, groupName, opponentStudentId } = req.body || {};
  if (!text && !fileUrl) {
    return res.status(400).json({ message: 'Matn yoki fayl yuborish kerak' });
  }

  const existing = await query(
    `SELECT id
     FROM weekly_challenge_entries
     WHERE challenge_id = $1 AND student_id = $2
     LIMIT 1`,
    [req.params.id, req.user.id],
  );

  if (existing.rowCount === 0) {
    await query(
      `INSERT INTO weekly_challenge_entries (
        id, challenge_id, student_id, text, file_url, group_name, opponent_student_id, status
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [
        createId(),
        req.params.id,
        req.user.id,
        text ? String(text).trim() : null,
        fileUrl ? String(fileUrl).trim() : null,
        groupName ? String(groupName).trim() : null,
        opponentStudentId && isId(String(opponentStudentId)) ? String(opponentStudentId) : null,
        'submitted',
      ],
    );
  } else {
    await query(
      `UPDATE weekly_challenge_entries
       SET text = COALESCE($2, text),
           file_url = COALESCE($3, file_url),
           group_name = COALESCE($4, group_name),
           opponent_student_id = COALESCE($5, opponent_student_id),
           status = 'submitted',
           updated_at = NOW()
       WHERE id = $1`,
      [
        existing.rows[0].id,
        text ? String(text).trim() : null,
        fileUrl ? String(fileUrl).trim() : null,
        groupName ? String(groupName).trim() : null,
        opponentStudentId && isId(String(opponentStudentId)) ? String(opponentStudentId) : null,
      ],
    );
  }

  await addNotification(
    challenge.created_by,
    'Yangi challenge topshirig\'i',
    'Talaba weekly challengega javob yubordi',
    'challenge_submission',
  );

  return res.status(201).json({ message: 'Challenge topshirig\'i yuborildi' });
});

router.patch('/challenges/entries/:id/review', requireAuth, requireRole(['admin', 'super']), async (req, res) => {
  if (!isId(req.params.id)) {
    return res.status(400).json({ message: 'Notogri ID' });
  }

  const entryRes = await query(
    `SELECT wce.*,
            wc.id AS challenge_id,
            wc.title AS challenge_title,
            wc.bonus_points,
            wc.created_by,
            s.curator_id
     FROM weekly_challenge_entries wce
     INNER JOIN weekly_challenges wc ON wc.id = wce.challenge_id
     INNER JOIN students s ON s.user_id = wce.student_id
     WHERE wce.id = $1
     LIMIT 1`,
    [req.params.id],
  );
  if (entryRes.rowCount === 0) {
    return res.status(404).json({ message: 'Challenge submission topilmadi' });
  }

  const entry = entryRes.rows[0];
  if (req.user.role === 'admin' && req.user.id !== entry.created_by && req.user.id !== entry.curator_id) {
    return res.status(403).json({ message: 'Ruxsat yoq' });
  }

  const { status, score, rankPosition, feedback } = req.body || {};
  const hasScore = hasOwn(req.body || {}, 'score');
  const hasRank = hasOwn(req.body || {}, 'rankPosition');
  const nextStatus = hasOwn(req.body || {}, 'status') ? String(status || '').toLowerCase() : String(entry.status || '').toLowerCase();

  if (nextStatus && !inArray(nextStatus, ['submitted', 'under_review', 'approved', 'graded', 'rejected'])) {
    return res.status(400).json({ message: 'Status notogri' });
  }

  const previousApplied = toInt(entry.points_applied, 0);
  const nextScore = hasScore ? Math.max(0, Math.min(100, toInt(score, 0))) : toInt(entry.score, 0);
  const nextRank = hasRank ? (rankPosition === null ? null : toInt(rankPosition, 0)) : (entry.rank_position ?? null);
  const bonus = challengeRankBonus(entry.bonus_points, nextRank);

  let nextApplied = previousApplied;
  if (inArray(nextStatus, ['approved', 'graded'])) {
    nextApplied = nextScore + bonus;
  } else if (nextStatus === 'rejected') {
    nextApplied = 0;
  }

  const delta = nextApplied - previousApplied;

  await query(
    `UPDATE weekly_challenge_entries
     SET status = COALESCE($2, status),
         score = COALESCE($3, score),
         rank_position = COALESCE($4, rank_position),
         feedback = COALESCE($5, feedback),
         points_applied = $6,
         updated_at = NOW()
     WHERE id = $1`,
    [
      req.params.id,
      hasOwn(req.body || {}, 'status') ? nextStatus : null,
      hasScore ? nextScore : null,
      hasRank && nextRank ? nextRank : null,
      hasOwn(req.body || {}, 'feedback') ? (feedback ? String(feedback).trim() : null) : null,
      nextApplied,
    ],
  );

  if (delta !== 0) {
    await applyPoints({
      studentId: entry.student_id,
      points: delta,
      sourceType: 'weekly_challenge',
      sourceId: entry.id,
      note: `${entry.challenge_title || 'Weekly challenge'} natijasi`,
      createdBy: req.user.id,
    });
  } else {
    await syncStudentGamification(entry.student_id);
  }

  if (inArray(nextStatus, ['approved', 'graded']) && [1, 2, 3].includes(toInt(nextRank, 0))) {
    await maybeCreateRankCertificate({
      studentId: entry.student_id,
      challengeId: entry.challenge_id,
      rankPosition: nextRank,
      issuedBy: req.user.id,
    });
    if (toInt(nextRank, 0) === 1) {
      await awardBadge(
        entry.student_id,
        {
          code: `challenge_win_${entry.challenge_id}`,
          name: 'Challenge 1-o\'rin',
          icon: 'emoji_events',
          description: 'Haftalik challenge g\'olibi',
        },
        { notify: false },
      );
    }
  }

  await addNotification(
    entry.student_id,
    'Challenge natijasi',
    `Status: ${nextStatus || entry.status}. Ball: ${nextScore}.`,
    'challenge_result',
  );

  const snapshot = await syncStudentGamification(entry.student_id);
  return res.json({
    message: 'Challenge baholandi',
    delta,
    score: snapshot.score,
    level: snapshot.level,
    streak: snapshot.streak,
  });
});

router.post('/challenges/entries/:id/like', requireAuth, async (req, res) => {
  if (!isId(req.params.id)) {
    return res.status(400).json({ message: 'Notogri ID' });
  }
  const exists = await query(
    'SELECT id FROM weekly_challenge_entries WHERE id = $1 LIMIT 1',
    [req.params.id],
  );
  if (exists.rowCount === 0) {
    return res.status(404).json({ message: 'Entry topilmadi' });
  }

  const current = await query(
    `SELECT id
     FROM challenge_entry_likes
     WHERE entry_id = $1 AND user_id = $2
     LIMIT 1`,
    [req.params.id, req.user.id],
  );

  let liked = false;
  if (current.rowCount > 0) {
    await query('DELETE FROM challenge_entry_likes WHERE id = $1', [current.rows[0].id]);
  } else {
    liked = true;
    await query(
      `INSERT INTO challenge_entry_likes (id, entry_id, user_id)
       VALUES ($1, $2, $3)`,
      [createId(), req.params.id, req.user.id],
    );
  }

  const countRes = await query(
    `SELECT COUNT(*)::int AS count
     FROM challenge_entry_likes
     WHERE entry_id = $1`,
    [req.params.id],
  );
  return res.json({
    liked,
    likes_count: toInt(countRes.rows[0]?.count, 0),
  });
});

router.get('/challenges/entries/:id/comments', requireAuth, async (req, res) => {
  if (!isId(req.params.id)) {
    return res.status(400).json({ message: 'Notogri ID' });
  }
  const comments = await query(
    `SELECT c.id, c.text, c.created_at, c.user_id, COALESCE(s.full_name, a.full_name, u.email) AS author_name
     FROM challenge_entry_comments c
     INNER JOIN users u ON u.id = c.user_id
     LEFT JOIN students s ON s.user_id = c.user_id
     LEFT JOIN admins a ON a.user_id = c.user_id
     WHERE c.entry_id = $1
     ORDER BY c.created_at DESC`,
    [req.params.id],
  );
  return res.json(comments.rows);
});

router.post('/challenges/entries/:id/comments', requireAuth, async (req, res) => {
  if (!isId(req.params.id)) {
    return res.status(400).json({ message: 'Notogri ID' });
  }
  const text = String(req.body?.text || '').trim();
  if (!text) {
    return res.status(400).json({ message: 'Komment matni kerak' });
  }
  await query(
    `INSERT INTO challenge_entry_comments (id, entry_id, user_id, text)
     VALUES ($1, $2, $3, $4)`,
    [createId(), req.params.id, req.user.id, text],
  );
  return res.status(201).json({ message: 'Komment qoshildi' });
});

router.get('/portfolio', requireAuth, async (req, res) => {
  await ensureMonthlyAwardsBackground();
  const requestedStudentId = req.query.studentId ? String(req.query.studentId) : null;
  const studentId = await resolveStudentIdByRole(req, res, requestedStudentId);
  if (!studentId) return;

  const student = await ensureStudentAccess(req, res, studentId);
  if (!student) return;

  const [taskRes, challengeRes, certRes] = await Promise.all([
    query(
      `SELECT ta.id AS assignment_id,
              ta.status,
              ta.graded_score,
              ta.feedback,
              ta.updated_at,
              t.id AS task_id,
              t.title AS task_title,
              t.category,
              t.deadline_at,
              latest.file_url,
              latest.text AS submission_text
       FROM task_assignments ta
       INNER JOIN tasks t ON t.id = ta.task_id
       LEFT JOIN (
         SELECT ts.assignment_id, ts.file_url, ts.text, ts.submitted_at
         FROM task_submissions ts
         INNER JOIN (
           SELECT assignment_id, MAX(submitted_at) AS latest_submitted_at
           FROM task_submissions
           GROUP BY assignment_id
         ) latest ON latest.assignment_id = ts.assignment_id
                AND latest.latest_submitted_at = ts.submitted_at
       ) latest ON latest.assignment_id = ta.id
       WHERE ta.student_id = $1
         AND (ta.graded_score IS NOT NULL OR ta.status IN ('graded', 'approved'))
       ORDER BY ta.updated_at DESC`,
      [studentId],
    ),
    query(
      `SELECT wce.id, wce.status, wce.score, wce.rank_position, wce.feedback,
              wce.file_url, wce.text, wce.updated_at,
              wc.title AS challenge_title, wc.category AS challenge_category
       FROM weekly_challenge_entries wce
       INNER JOIN weekly_challenges wc ON wc.id = wce.challenge_id
       WHERE wce.student_id = $1
         AND wce.status IN ('approved', 'graded')
       ORDER BY wce.updated_at DESC`,
      [studentId],
    ),
    query(
      `SELECT id, title, rank_label, award_type, award_month_key, award_rank,
              note, template_name, template_url, pdf_url, issued_at
       FROM certificates
       WHERE student_id = $1
       ORDER BY issued_at DESC`,
      [studentId],
    ),
  ]);

  return res.json({
    student_id: studentId,
    full_name: student.full_name,
    tasks: taskRes.rows,
    challenges: challengeRes.rows,
    certificates: certRes.rows,
    summary: {
      tasks_count: taskRes.rowCount,
      challenges_count: challengeRes.rowCount,
      certificates_count: certRes.rowCount,
      level: levelInfo(student.spirituality_score || 0),
    },
  });
});

router.get('/certificates', requireAuth, async (req, res) => {
  await ensureMonthlyAwardsBackground();
  let studentId = req.user.id;
  if (req.user.role !== 'student' && req.query.studentId) {
    const requested = String(req.query.studentId);
    if (!isId(requested)) {
      return res.status(400).json({ message: 'Notogri ID' });
    }
    const student = await ensureStudentAccess(req, res, requested);
    if (!student) return;
    studentId = requested;
  } else if (req.user.role !== 'student' && !req.query.studentId) {
    return res.status(400).json({ message: 'studentId kerak' });
  }

  const result = await query(
    `SELECT c.*,
            COALESCE(a.full_name, u.email) AS issued_by_name
     FROM certificates c
     INNER JOIN users u ON u.id = c.issued_by
     LEFT JOIN admins a ON a.user_id = c.issued_by
     WHERE c.student_id = $1
     ORDER BY c.issued_at DESC`,
    [studentId],
  );

  return res.json(result.rows);
});

router.post('/certificates', requireAuth, requireRole(['admin', 'super']), async (req, res) => {
  const {
    studentId,
    title,
    rankLabel,
    note,
    challengeId,
    templateName,
    templateUrl,
    pdfUrl,
    bonusPoints,
  } = req.body || {};

  if (!isId(String(studentId || ''))) {
    return res.status(400).json({ message: 'Talaba tanlanmagan' });
  }
  if (!title) {
    return res.status(400).json({ message: 'Sertifikat sarlavhasi kerak' });
  }
  const student = await ensureStudentAccess(req, res, String(studentId));
  if (!student) return;

  if (challengeId && !isId(String(challengeId))) {
    return res.status(400).json({ message: 'Challenge ID notogri' });
  }

  const certificateId = createId();
  await query(
    `INSERT INTO certificates (
      id, student_id, challenge_id, issued_by, title, rank_label, note, template_name, template_url, pdf_url
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
    [
      certificateId,
      String(studentId),
      challengeId ? String(challengeId) : null,
      req.user.id,
      String(title).trim(),
      rankLabel ? String(rankLabel).trim() : null,
      note ? String(note).trim() : null,
      templateName ? String(templateName).trim() : null,
      templateUrl ? String(templateUrl).trim() : null,
      pdfUrl ? String(pdfUrl).trim() : null,
    ],
  );

  const bonus = Math.max(0, toInt(bonusPoints, 0));
  if (bonus > 0) {
    await applyPoints({
      studentId: String(studentId),
      points: bonus,
      sourceType: 'certificate_bonus',
      sourceId: certificateId,
      note: `Sertifikat bonusi: ${title}`,
      createdBy: req.user.id,
    });
  } else {
    await syncStudentGamification(String(studentId));
  }

  await addNotification(
    String(studentId),
    'Yangi sertifikat',
    `${String(title).trim()} sertifikati berildi`,
    'certificate',
  );

  return res.status(201).json({
    id: certificateId,
    message: 'Sertifikat berildi',
  });
});

router.get('/reminders', requireAuth, async (req, res) => {
  if (req.user.role === 'student') {
    const stats = await fetchPendingTaskStats(req.user.id);
    return res.json(stats);
  }

  const params = [];
  let where = '';
  if (req.user.role === 'admin') {
    params.push(req.user.id);
    where = 'WHERE s.curator_id = $1';
  } else if (req.query.curatorId && isId(String(req.query.curatorId))) {
    params.push(String(req.query.curatorId));
    where = 'WHERE s.curator_id = $1';
  }

  const students = await query(
    `SELECT s.user_id, s.full_name
     FROM students s
     ${where}
     ORDER BY s.full_name ASC`,
    params,
  );

  const rows = [];
  for (const student of students.rows) {
    const stats = await fetchPendingTaskStats(student.user_id);
    rows.push({
      student_id: student.user_id,
      full_name: student.full_name,
      ...stats,
    });
  }
  return res.json(rows);
});

router.post('/reminders/send', requireAuth, requireRole(['admin', 'super']), async (req, res) => {
  const params = [];
  let where = '';
  if (req.user.role === 'admin') {
    params.push(req.user.id);
    where = 'WHERE s.curator_id = $1';
  } else if (req.body?.curatorId && isId(String(req.body.curatorId))) {
    params.push(String(req.body.curatorId));
    where = 'WHERE s.curator_id = $1';
  }

  const students = await query(
    `SELECT s.user_id
     FROM students s
     ${where}`,
    params,
  );

  let sent = 0;
  for (const student of students.rows) {
    const stats = await fetchPendingTaskStats(student.user_id);
    if (stats.pending_count < 1) continue;
    const body =
      stats.due_soon_count > 0
        ? `Sizga ${stats.pending_count} ta vazifa qoldi, ${stats.due_soon_count} tasi tez orada tugaydi`
        : `Sizga ${stats.pending_count} ta vazifa qoldi`;
    await addNotification(student.user_id, 'Vazifa eslatmasi', body, 'reminder');
    sent += 1;
  }

  return res.json({ message: 'Eslatmalar yuborildi', sent });
});

router.post('/system/monthly-awards/run', requireAuth, requireRole(['super']), async (req, res) => {
  const result = await runMonthlyAwardsIfDue({ force: true });
  return res.json(result);
});

module.exports = router;
