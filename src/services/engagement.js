const fs = require('fs');
const path = require('path');

const { createId, query } = require('../db');

const CERTIFICATE_TEMPLATE_DIR = path.join(__dirname, '..', 'certifikat');
const CERTIFICATE_TEMPLATE_BASE_URL = '/certificates/templates';
const MONTHLY_AWARD_ISSUER_FALLBACK_ID = process.env.SYSTEM_AWARD_ISSUER_ID || null;

const LEVEL_STEP = 20;

const BADGE_RULES = [
  {
    code: 'score_10',
    type: 'score',
    threshold: 10,
    name: 'First Step',
    icon: 'star-outline',
    description: '10+ points collected',
  },
  {
    code: 'score_30',
    type: 'score',
    threshold: 30,
    name: 'Active Student',
    icon: 'flash',
    description: '30+ points collected',
  },
  {
    code: 'score_60',
    type: 'score',
    threshold: 60,
    name: 'Impact Leader',
    icon: 'trophy',
    description: '60+ points collected',
  },
  {
    code: 'score_100',
    type: 'score',
    threshold: 100,
    name: 'Campus Legend',
    icon: 'crown',
    description: '100+ points collected',
  },
  {
    code: 'streak_2',
    type: 'streak',
    threshold: 2,
    name: 'Two Week Sprint',
    icon: 'timeline',
    description: '2 week streak reached',
  },
  {
    code: 'streak_4',
    type: 'streak',
    threshold: 4,
    name: 'Consistency Master',
    icon: 'whatshot',
    description: '4 week streak reached',
  },
  {
    code: 'challenge_winner',
    type: 'wins',
    threshold: 1,
    name: 'Weekly Champion',
    icon: 'military_tech',
    description: 'Won at least one weekly challenge',
  },
  {
    code: 'certified',
    type: 'certificates',
    threshold: 1,
    name: 'Certified Talent',
    icon: 'workspace_premium',
    description: 'Received at least one certificate',
  },
];

function toInt(value, fallback = 0) {
  if (value === null || value === undefined || value === '') {
    return fallback;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.trunc(parsed);
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function normalizeLimit(raw, min = 1, max = 50) {
  return clamp(toInt(raw, 10), min, max);
}

function monthKey(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  return `${year}-${month}`;
}

function shiftMonth(date, delta) {
  return new Date(date.getFullYear(), date.getMonth() + delta, 1);
}

function monthBounds(monthDate) {
  const start = new Date(monthDate.getFullYear(), monthDate.getMonth(), 1, 0, 0, 0, 0);
  const end = new Date(monthDate.getFullYear(), monthDate.getMonth() + 1, 0, 23, 59, 59, 999);
  return { start, end };
}

function monthFromKey(raw) {
  if (!raw || typeof raw !== 'string') return null;
  const [yearRaw, monthRaw] = raw.split('-');
  const year = Number(yearRaw);
  const month = Number(monthRaw);
  if (!Number.isFinite(year) || !Number.isFinite(month)) return null;
  if (month < 1 || month > 12) return null;
  return new Date(year, month - 1, 1);
}

function monthKeyDiff(a, b) {
  const ad = monthFromKey(a);
  const bd = monthFromKey(b);
  if (!ad || !bd) return Number.NaN;
  return (ad.getFullYear() - bd.getFullYear()) * 12 + (ad.getMonth() - bd.getMonth());
}

function isConsecutiveMonthKeys(keys) {
  if (!Array.isArray(keys) || keys.length < 2) return true;
  for (let i = 0; i < keys.length - 1; i += 1) {
    if (monthKeyDiff(keys[i], keys[i + 1]) !== 1) {
      return false;
    }
  }
  return true;
}

function formatDateKey(date) {
  const year = String(date.getFullYear()).padStart(4, '0');
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function toDate(raw) {
  if (!raw) return null;
  const parsed = raw instanceof Date ? raw : new Date(raw);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed;
}

function weekStartMonday(date) {
  const value = new Date(date.getFullYear(), date.getMonth(), date.getDate());
  const day = value.getDay();
  const diff = day === 0 ? -6 : 1 - day;
  value.setDate(value.getDate() + diff);
  value.setHours(0, 0, 0, 0);
  return value;
}

function addDays(date, days) {
  const next = new Date(date.getTime());
  next.setDate(next.getDate() + days);
  return next;
}

function levelInfo(score) {
  const safeScore = Math.max(0, toInt(score, 0));
  const level = Math.floor(safeScore / LEVEL_STEP) + 1;
  const currentFloor = (level - 1) * LEVEL_STEP;
  const nextLevelAt = level * LEVEL_STEP;
  const progress = clamp((safeScore - currentFloor) / LEVEL_STEP, 0, 1);
  return {
    level,
    score: safeScore,
    nextLevelAt,
    progressPercent: Math.round(progress * 100),
  };
}

async function getStudentScore(studentId) {
  if (!studentId) return 0;
  const result = await query(
    `SELECT spirituality_score
     FROM students
     WHERE user_id = $1
     LIMIT 1`,
    [studentId],
  );
  if (result.rowCount === 0) return 0;
  return toInt(result.rows[0].spirituality_score, 0);
}

async function insertBadge(studentId, { code, name, icon, description }) {
  await query(
    `INSERT INTO student_badges (
      id, student_id, badge_code, badge_name, badge_icon, badge_description
    ) VALUES ($1, $2, $3, $4, $5, $6)
    ON DUPLICATE KEY UPDATE id = id`,
    [createId(), studentId, code, name, icon ?? null, description ?? null],
  );
}

async function awardBadge(studentId, badge, { notify = false } = {}) {
  if (!studentId || !badge?.code || !badge?.name) return false;
  const existing = await query(
    `SELECT id
     FROM student_badges
     WHERE student_id = $1 AND badge_code = $2
     LIMIT 1`,
    [studentId, badge.code],
  );
  if (existing.rowCount > 0) return false;

  await insertBadge(studentId, badge);
  if (notify) {
    await addNotification(
      studentId,
      'Yangi badge',
      `${badge.name} badge berildi`,
      'badge',
    );
  }
  return true;
}

async function addNotification(userId, title, body, type = 'engagement') {
  if (!userId) return;
  await query(
    `INSERT INTO notifications (id, user_id, type, title, body)
     VALUES ($1, $2, $3, $4, $5)`,
    [createId(), userId, type, title ?? null, body ?? null],
  );
}

async function calculateWeeklyStreak(studentId) {
  if (!studentId) return 0;
  const result = await query(
    `SELECT DISTINCT DATE_SUB(DATE(activity_at), INTERVAL WEEKDAY(activity_at) DAY) AS week_start
     FROM (
       SELECT ts.submitted_at AS activity_at
       FROM task_submissions ts
       INNER JOIN task_assignments ta ON ta.id = ts.assignment_id
       WHERE ta.student_id = $1
       UNION ALL
       SELECT wce.created_at AS activity_at
       FROM weekly_challenge_entries wce
       WHERE wce.student_id = $1
       UNION ALL
       SELECT spl.created_at AS activity_at
       FROM student_points_ledger spl
       WHERE spl.student_id = $1
     ) activity
     WHERE activity_at IS NOT NULL
     ORDER BY week_start DESC`,
    [studentId],
  );

  if (result.rowCount === 0) return 0;

  const weekSet = new Set(
    result.rows
      .map((row) => toDate(row.week_start))
      .filter((date) => !!date)
      .map((date) => formatDateKey(date)),
  );

  let streak = 0;
  let expected = weekStartMonday(new Date());
  while (weekSet.has(formatDateKey(expected))) {
    streak += 1;
    expected = addDays(expected, -7);
  }

  return streak;
}

async function fetchBadgeMetrics(studentId) {
  const [winsRes, certRes] = await Promise.all([
    query(
      `SELECT COUNT(*)::int AS count
       FROM weekly_challenge_entries
       WHERE student_id = $1
         AND rank_position = 1
         AND status IN ('approved', 'graded')`,
      [studentId],
    ),
    query(
      `SELECT COUNT(*)::int AS count
       FROM certificates
       WHERE student_id = $1`,
      [studentId],
    ),
  ]);

  return {
    wins: toInt(winsRes.rows[0]?.count, 0),
    certificates: toInt(certRes.rows[0]?.count, 0),
  };
}

async function ensureMilestoneBadges(studentId, snapshot) {
  if (!studentId) return;
  const score = Math.max(0, toInt(snapshot?.score, 0));
  const streak = Math.max(0, toInt(snapshot?.streak, 0));
  const metrics = await fetchBadgeMetrics(studentId);

  for (const rule of BADGE_RULES) {
    let value = 0;
    if (rule.type === 'score') value = score;
    if (rule.type === 'streak') value = streak;
    if (rule.type === 'wins') value = metrics.wins;
    if (rule.type === 'certificates') value = metrics.certificates;
    if (value >= rule.threshold) {
      await insertBadge(studentId, rule);
    }
  }
}

async function getStudentBadges(studentId, limit = 20) {
  const safeLimit = normalizeLimit(limit, 1, 100);
  const result = await query(
    `SELECT badge_code, badge_name, badge_icon, badge_description, awarded_at
     FROM student_badges
     WHERE student_id = $1
     ORDER BY awarded_at DESC
     LIMIT $2`,
    [studentId, safeLimit],
  );
  return result.rows.map((row) => ({
    code: row.badge_code,
    name: row.badge_name,
    icon: row.badge_icon,
    description: row.badge_description,
    awarded_at: row.awarded_at,
  }));
}

async function syncStudentGamification(studentId) {
  const score = await getStudentScore(studentId);
  const streak = await calculateWeeklyStreak(studentId);
  await ensureMilestoneBadges(studentId, { score, streak });
  const badges = await getStudentBadges(studentId, 50);
  return {
    score,
    streak,
    level: levelInfo(score),
    badges,
  };
}

async function applyPoints({
  studentId,
  points,
  sourceType = 'manual',
  sourceId = null,
  note = null,
  createdBy = null,
}) {
  if (!studentId) {
    return { delta: 0, ...levelInfo(0), score: 0, badges: [], streak: 0 };
  }

  const delta = toInt(points, 0);
  if (delta !== 0) {
    await query(
      `UPDATE students
       SET spirituality_score = GREATEST(0, spirituality_score + $2)
       WHERE user_id = $1`,
      [studentId, delta],
    );
    await query(
      `INSERT INTO student_points_ledger (
        id, student_id, source_type, source_id, points, note, created_by
      ) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [
        createId(),
        studentId,
        sourceType,
        sourceId ?? null,
        delta,
        note ?? null,
        createdBy ?? null,
      ],
    );
  }

  const snapshot = await syncStudentGamification(studentId);
  return { delta, ...snapshot };
}

async function getLeaderboard({
  limit = 10,
  curatorId = null,
  includeAll = false,
} = {}) {
  const params = [];
  let curatorClause = '';
  if (curatorId) {
    params.push(curatorId);
    curatorClause = `AND s.curator_id = $${params.length}`;
  }
  let limitClause = '';
  if (!includeAll) {
    const safeLimit = normalizeLimit(limit, 1, 1000);
    params.push(safeLimit);
    limitClause = `LIMIT $${params.length}`;
  }

  const result = await query(
    `SELECT s.user_id AS student_id,
            s.full_name,
            s.spirituality_score,
            COUNT(sb.id)::int AS badge_count
     FROM students s
     INNER JOIN users u ON u.id = s.user_id
     LEFT JOIN student_badges sb ON sb.student_id = s.user_id
     WHERE u.role = 'student'
       AND u.status = 'active'
       ${curatorClause}
     GROUP BY s.user_id, s.full_name, s.spirituality_score
     ORDER BY s.spirituality_score DESC, badge_count DESC, s.full_name ASC
     ${limitClause}`,
    params,
  );

  return result.rows.map((row, index) => ({
    rank: index + 1,
    student_id: row.student_id,
    full_name: row.full_name,
    score: toInt(row.spirituality_score, 0),
    badge_count: toInt(row.badge_count, 0),
    level: levelInfo(row.spirituality_score).level,
  }));
}

async function getStudentRank(studentId, { curatorId = null } = {}) {
  if (!studentId) return null;
  const params = [studentId];
  let scopeClause = '';
  if (curatorId) {
    params.push(curatorId);
    scopeClause = 'AND s.curator_id = $2';
  }
  const rankRes = await query(
    `SELECT 1 + COUNT(*)::int AS rank_position
     FROM students s
     WHERE s.spirituality_score > (
       SELECT spirituality_score
       FROM students
       WHERE user_id = $1
       LIMIT 1
     )
     ${scopeClause}`,
    params,
  );
  return toInt(rankRes.rows[0]?.rank_position, 0) || null;
}

function buildMonthSeries(map, startMonth, months) {
  const labels = [];
  const values = [];
  for (let i = 0; i < months; i += 1) {
    const date = shiftMonth(startMonth, i);
    const key = monthKey(date);
    labels.push(key);
    values.push(toInt(map.get(key), 0));
  }
  return { labels, values };
}

async function getStudentProgress(studentId, months = 6) {
  const safeMonths = normalizeLimit(months, 3, 18);
  const now = new Date();
  const startMonth = shiftMonth(new Date(now.getFullYear(), now.getMonth(), 1), -(safeMonths - 1));
  const startDate = new Date(startMonth.getFullYear(), startMonth.getMonth(), 1, 0, 0, 0);
  const endDate = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 23, 59, 59, 999);

  const [pointsRes, submissionRes] = await Promise.all([
    query(
      `SELECT EXTRACT(YEAR FROM created_at)::int AS year,
              EXTRACT(MONTH FROM created_at)::int AS month,
              COALESCE(SUM(points), 0)::int AS points
       FROM student_points_ledger
       WHERE student_id = $1
         AND created_at BETWEEN $2 AND $3
       GROUP BY year, month`,
      [studentId, startDate, endDate],
    ),
    query(
      `SELECT EXTRACT(YEAR FROM ts.submitted_at)::int AS year,
              EXTRACT(MONTH FROM ts.submitted_at)::int AS month,
              COUNT(*)::int AS count
       FROM task_submissions ts
       INNER JOIN task_assignments ta ON ta.id = ts.assignment_id
       WHERE ta.student_id = $1
         AND ts.submitted_at BETWEEN $2 AND $3
       GROUP BY year, month`,
      [studentId, startDate, endDate],
    ),
  ]);

  const pointsMap = new Map();
  for (const row of pointsRes.rows) {
    pointsMap.set(`${row.year}-${String(row.month).padStart(2, '0')}`, toInt(row.points, 0));
  }

  const submissionMap = new Map();
  for (const row of submissionRes.rows) {
    submissionMap.set(`${row.year}-${String(row.month).padStart(2, '0')}`, toInt(row.count, 0));
  }

  return {
    points: buildMonthSeries(pointsMap, startMonth, safeMonths),
    submissions: buildMonthSeries(submissionMap, startMonth, safeMonths),
  };
}

let certificateTemplateCache = {
  loadedAt: 0,
  templates: null,
};

function pickTemplateFile(files, patterns) {
  if (!Array.isArray(files)) return null;
  const lower = files.map((name) => ({ name, value: name.toLowerCase() }));
  for (const pattern of patterns) {
    const found = lower.find((file) => pattern.test(file.value));
    if (found) return found.name;
  }
  return null;
}

function resolveMonthlyCertificateTemplates(forceReload = false) {
  const now = Date.now();
  const cacheTtlMs = 60 * 1000;
  if (
    !forceReload &&
    certificateTemplateCache.templates &&
    now - certificateTemplateCache.loadedAt < cacheTtlMs
  ) {
    return certificateTemplateCache.templates;
  }

  let files = [];
  try {
    const stat = fs.existsSync(CERTIFICATE_TEMPLATE_DIR)
      ? fs.statSync(CERTIFICATE_TEMPLATE_DIR)
      : null;
    if (!stat || !stat.isDirectory()) {
      certificateTemplateCache = { loadedAt: now, templates: null };
      return null;
    }
    files = fs
      .readdirSync(CERTIFICATE_TEMPLATE_DIR)
      .filter((name) => /\.pdf$/i.test(name));
  } catch (_) {
    certificateTemplateCache = { loadedAt: now, templates: null };
    return null;
  }

  const sortedFiles = [...files].sort((a, b) => a.localeCompare(b));
  let first = pickTemplateFile(sortedFiles, [/^1/i, /1[-_\s]?daraj/i, /bir/i, /first/i]);
  let second = pickTemplateFile(sortedFiles, [/^2/i, /2[-_\s]?daraj/i, /ikki/i, /second/i]);
  let third = pickTemplateFile(sortedFiles, [/^3/i, /3[-_\s]?daraj/i, /uch/i, /third/i]);
  const top = pickTemplateFile(files, [/oliy/i, /grand/i, /supreme/i, /top/i]);

  if (!first && sortedFiles[0]) first = sortedFiles[0];
  if (!second && sortedFiles[1]) second = sortedFiles[1];
  if (!third && sortedFiles[2]) third = sortedFiles[2];

  const toUrl = (name) =>
    name ? `${CERTIFICATE_TEMPLATE_BASE_URL}/${encodeURIComponent(name)}` : null;
  const templates = {
    first: toUrl(first),
    second: toUrl(second),
    third: toUrl(third),
    top: toUrl(top),
  };

  certificateTemplateCache = { loadedAt: now, templates };
  return templates;
}

async function resolveAwardIssuerId() {
  if (MONTHLY_AWARD_ISSUER_FALLBACK_ID) {
    return MONTHLY_AWARD_ISSUER_FALLBACK_ID;
  }

  const superRes = await query(
    `SELECT id
     FROM users
     WHERE role = 'super'
     ORDER BY created_at ASC
     LIMIT 1`,
  );
  if (superRes.rowCount > 0) {
    return superRes.rows[0].id;
  }

  const adminRes = await query(
    `SELECT id
     FROM users
     WHERE role IN ('admin', 'super')
     ORDER BY created_at ASC
     LIMIT 1`,
  );
  return adminRes.rowCount > 0 ? adminRes.rows[0].id : null;
}

async function acquireMonthlyAwardRun(monthKeyValue) {
  try {
    await query(
      `INSERT INTO monthly_award_runs (id, month_key, status, message)
       VALUES ($1, $2, $3, $4)`,
      [createId(), monthKeyValue, 'processing', 'Auto monthly award run'],
    );
    return true;
  } catch (_) {
    return false;
  }
}

async function finalizeMonthlyAwardRun(monthKeyValue, status, message) {
  await query(
    `UPDATE monthly_award_runs
     SET status = $2,
         message = $3,
         processed_at = NOW()
     WHERE month_key = $1`,
    [monthKeyValue, status, message ?? null],
  );
}

async function insertMonthlyLeaderboardRows(monthKeyValue, rows) {
  for (let index = 0; index < rows.length; index += 1) {
    const item = rows[index];
    await query(
      `INSERT INTO monthly_leaderboard (
        id, month_key, student_id, rank_position, score_value
      ) VALUES ($1, $2, $3, $4, $5)
      ON DUPLICATE KEY UPDATE score_value = VALUES(score_value)`,
      [
        createId(),
        monthKeyValue,
        item.student_id,
        index + 1,
        toInt(item.score, 0),
      ],
    );
  }
}

function monthlyRankTemplateUrl(templates, rank) {
  if (!templates) return null;
  if (rank === 1) return templates.first || null;
  if (rank === 2) return templates.second || null;
  if (rank === 3) return templates.third || null;
  return null;
}

async function createMonthlyRankCertificate({
  monthKeyValue,
  rank,
  studentId,
  fullName,
  score,
  issuedBy,
  templates,
}) {
  const exists = await query(
    `SELECT id
     FROM certificates
     WHERE student_id = $1
       AND award_type = 'monthly_rank'
       AND award_month_key = $2
       AND award_rank = $3
     LIMIT 1`,
    [studentId, monthKeyValue, rank],
  );
  if (exists.rowCount > 0) return false;

  const templateUrl = monthlyRankTemplateUrl(templates, rank);
  const rankLabel = `${rank}-o'rin`;
  await query(
    `INSERT INTO certificates (
      id, student_id, issued_by, title, rank_label,
      award_type, award_month_key, award_rank,
      note, template_name, template_url, pdf_url
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
    [
      createId(),
      studentId,
      issuedBy,
      'Oy yakuni g\'olibi',
      rankLabel,
      'monthly_rank',
      monthKeyValue,
      rank,
      `${monthKeyValue} oy natijasi. Ball: ${toInt(score, 0)}.`,
      `monthly-rank-${rank}`,
      templateUrl,
      templateUrl,
    ],
  );

  await addNotification(
    studentId,
    'Oy yakuni sertifikati',
    `Tabriklaymiz ${fullName || 'talaba'}: ${rankLabel} sertifikat berildi.`,
    'monthly_award',
  );

  return true;
}

async function maybeAwardTopStreakCertificate({ studentId, issuedBy, templates, monthKeyValue }) {
  if (!studentId) return false;
  const topRes = await query(
    `SELECT month_key, student_id
     FROM monthly_leaderboard
     WHERE rank_position = 1
     ORDER BY month_key DESC
     LIMIT 3`,
  );
  if (topRes.rowCount < 3) return false;

  const rows = topRes.rows;
  const monthKeys = rows.map((row) => row.month_key);
  const sameStudent = rows.every((row) => row.student_id === studentId);
  if (!sameStudent || !isConsecutiveMonthKeys(monthKeys)) {
    return false;
  }

  const exists = await query(
    `SELECT id
     FROM certificates
     WHERE student_id = $1
       AND award_type = 'top_streak'
       AND award_month_key = $2
     LIMIT 1`,
    [studentId, monthKeyValue],
  );
  if (exists.rowCount > 0) return false;

  const monthStart = monthKeys[2];
  const monthEnd = monthKeys[0];
  const templateUrl = templates?.top || null;
  await query(
    `INSERT INTO certificates (
      id, student_id, issued_by, title, rank_label,
      award_type, award_month_key, award_rank,
      note, template_name, template_url, pdf_url
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
    [
      createId(),
      studentId,
      issuedBy,
      'Oliy sertifikat',
      'Top 1 (3 oy ketma-ket)',
      'top_streak',
      monthKeyValue,
      1,
      `${monthStart} - ${monthEnd} oralig'ida 3 oy ketma-ket 1-o'rin.`,
      'oliy-sertifikat',
      templateUrl,
      templateUrl,
    ],
  );

  await awardBadge(
    studentId,
    {
      code: `top_streak_${monthKeyValue}`,
      name: 'Oliy Sertifikat',
      icon: 'verified',
      description: '3 oy ketma-ket 1-o\'rin',
    },
    { notify: false },
  );

  await addNotification(
    studentId,
    'Oliy sertifikat',
    '3 oy ketma-ket 1-o\'rin uchun Oliy sertifikat berildi.',
    'monthly_award',
  );

  return true;
}

async function fetchMonthlyTopStudents(monthDate) {
  const { start, end } = monthBounds(monthDate);
  const result = await query(
    `SELECT s.user_id AS student_id,
            s.full_name,
            s.spirituality_score AS score
     FROM students s
     INNER JOIN users u ON u.id = s.user_id
     WHERE u.role = 'student'
       AND u.status = 'active'
       AND s.spirituality_score > 0
       AND s.user_id IN (
         SELECT DISTINCT student_id
         FROM student_points_ledger
         WHERE created_at BETWEEN $1 AND $2
       )
     ORDER BY s.spirituality_score DESC, s.full_name ASC
     LIMIT 3`,
    [start, end],
  );

  if (result.rowCount > 0) {
    return result.rows.map((row) => ({
      student_id: row.student_id,
      full_name: row.full_name,
      score: toInt(row.score, 0),
    }));
  }

  const fallback = await query(
    `SELECT s.user_id AS student_id,
            s.full_name,
            s.spirituality_score AS score
     FROM students s
     INNER JOIN users u ON u.id = s.user_id
     WHERE u.role = 'student'
       AND u.status = 'active'
       AND s.spirituality_score > 0
     ORDER BY s.spirituality_score DESC, s.full_name ASC
     LIMIT 3`,
  );

  return fallback.rows.map((row) => ({
    student_id: row.student_id,
    full_name: row.full_name,
    score: toInt(row.score, 0),
  }));
}

async function runMonthlyAwardsIfDue({ now = new Date(), force = false } = {}) {
  const reference = now instanceof Date ? now : new Date(now);
  if (Number.isNaN(reference.getTime())) {
    return { processed: false, reason: 'invalid_date' };
  }

  const targetMonth = shiftMonth(new Date(reference.getFullYear(), reference.getMonth(), 1), -1);
  const monthKeyValue = monthKey(targetMonth);

  if (!force) {
    const acquired = await acquireMonthlyAwardRun(monthKeyValue);
    if (!acquired) {
      return { processed: false, reason: 'already_processed', monthKey: monthKeyValue };
    }
  } else {
    const acquired = await acquireMonthlyAwardRun(monthKeyValue);
    if (!acquired) {
      const existing = await query(
        `SELECT id, status
         FROM monthly_award_runs
         WHERE month_key = $1
         LIMIT 1`,
        [monthKeyValue],
      );
      if (existing.rowCount > 0 && existing.rows[0].status === 'failed') {
        await query(
          `UPDATE monthly_award_runs
           SET status = 'processing',
               message = 'Force rerun',
               processed_at = NULL
           WHERE month_key = $1`,
          [monthKeyValue],
        );
      } else if (existing.rowCount > 0) {
        return { processed: false, reason: 'already_processed', monthKey: monthKeyValue };
      }
    }
  }

  try {
    const templates = resolveMonthlyCertificateTemplates();
    const issuedBy = await resolveAwardIssuerId();
    if (!issuedBy) {
      await finalizeMonthlyAwardRun(monthKeyValue, 'failed', 'Award issuer not found');
      return { processed: false, reason: 'issuer_not_found', monthKey: monthKeyValue };
    }

    const topStudents = await fetchMonthlyTopStudents(targetMonth);
    await insertMonthlyLeaderboardRows(monthKeyValue, topStudents);

    let certificatesIssued = 0;
    let streakIssued = false;
    for (let index = 0; index < topStudents.length; index += 1) {
      const rank = index + 1;
      const student = topStudents[index];
      const created = await createMonthlyRankCertificate({
        monthKeyValue,
        rank,
        studentId: student.student_id,
        fullName: student.full_name,
        score: student.score,
        issuedBy,
        templates,
      });
      if (created) {
        certificatesIssued += 1;
      }

      if (rank === 1) {
        streakIssued = await maybeAwardTopStreakCertificate({
          studentId: student.student_id,
          issuedBy,
          templates,
          monthKeyValue,
        });
      }
    }

    await finalizeMonthlyAwardRun(
      monthKeyValue,
      'completed',
      `Winners: ${topStudents.length}. Certificates: ${certificatesIssued}. Oliy: ${streakIssued ? 'yes' : 'no'}`,
    );

    return {
      processed: true,
      monthKey: monthKeyValue,
      winners: topStudents,
      certificatesIssued,
      streakIssued,
    };
  } catch (error) {
    await finalizeMonthlyAwardRun(monthKeyValue, 'failed', error?.message || 'Unknown error');
    return {
      processed: false,
      reason: 'failed',
      monthKey: monthKeyValue,
      error: error?.message || 'Unknown error',
    };
  }
}

async function syncLegacyTaskGradePoints(limit = 500) {
  const safeLimit = normalizeLimit(limit, 50, 5000);
  const rowsRes = await query(
    `SELECT ta.id, ta.student_id, ta.graded_score, ta.points_applied, t.title
     FROM task_assignments ta
     INNER JOIN tasks t ON t.id = ta.task_id
     WHERE ta.graded_score IS NOT NULL
       AND ta.graded_score > 0
       AND (ta.points_applied IS NULL OR ta.points_applied = 0)
     ORDER BY ta.updated_at ASC
     LIMIT $1`,
    [safeLimit],
  );

  let migrated = 0;
  for (const row of rowsRes.rows) {
    const points = toInt(row.graded_score, 0);
    if (points < 1) continue;

    await query(
      `UPDATE task_assignments
       SET points_applied = $2
       WHERE id = $1`,
      [row.id, points],
    );

    await applyPoints({
      studentId: row.student_id,
      points,
      sourceType: 'task_grade_migration',
      sourceId: row.id,
      note: `${row.title || 'Topshiriq'} eski baho migratsiyasi`,
      createdBy: null,
    });
    migrated += 1;
  }

  return { migrated };
}

let monthlyAwardTimer = null;

function startMonthlyAwardsScheduler({ intervalMs = 1000 * 60 * 60 * 6 } = {}) {
  if (monthlyAwardTimer) return monthlyAwardTimer;

  const safeInterval = Math.max(1000 * 60 * 30, toInt(intervalMs, 0));
  const run = async () => {
    try {
      await runMonthlyAwardsIfDue();
    } catch (_) {}
  };

  run();
  monthlyAwardTimer = setInterval(run, safeInterval);
  if (typeof monthlyAwardTimer.unref === 'function') {
    monthlyAwardTimer.unref();
  }
  return monthlyAwardTimer;
}

module.exports = {
  BADGE_RULES,
  LEVEL_STEP,
  addNotification,
  applyPoints,
  awardBadge,
  calculateWeeklyStreak,
  getLeaderboard,
  getStudentBadges,
  getStudentProgress,
  getStudentRank,
  getStudentScore,
  levelInfo,
  normalizeLimit,
  runMonthlyAwardsIfDue,
  resolveMonthlyCertificateTemplates,
  startMonthlyAwardsScheduler,
  syncStudentGamification,
  syncLegacyTaskGradePoints,
  toInt,
};
