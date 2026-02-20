const { randomUUID } = require('crypto');
const mysql = require('mysql2/promise');
const bcrypt = require('bcryptjs');

let pool;

function createId() {
  return randomUUID();
}

function normalizeSql(sql) {
  return sql
    .replace(/\$(\d+)/g, '?')
    .replace(/::int/g, '')
    .replace(/::text\[\]/g, '')
    .replace(/::jsonb/g, '')
    .replace(/\s+NULLS LAST/gi, '')
    .replace(/=\s*ANY\(\?\)/gi, 'IN (?)');
}

async function query(text, params = []) {
  if (!pool) {
    throw new Error('Database is not connected');
  }

  const orderedParams = [];
  const placeholderRegex = /\$(\d+)/g;
  let match = placeholderRegex.exec(text);
  while (match) {
    const index = Number(match[1]) - 1;
    orderedParams.push(params[index]);
    match = placeholderRegex.exec(text);
  }

  const hasReturning = /\bRETURNING\s+\*/i.test(text);
  let sql = normalizeSql(text);

  if (hasReturning) {
    sql = sql.replace(/\bRETURNING\s+\*/gi, '');
  }

  const [rows] = await pool.query(sql, orderedParams.length > 0 ? orderedParams : params);

  if (hasReturning) {
    const tableMatch = text.match(/INSERT\s+INTO\s+([a-z_]+)/i);
    const table = tableMatch?.[1];
    const id = params?.[0];
    if (!table || !id) {
      return { rows: [], rowCount: 0 };
    }
    const [returnRows] = await pool.query(`SELECT * FROM ${table} WHERE id = ? LIMIT 1`, [id]);
    return { rows: returnRows, rowCount: returnRows.length };
  }

  if (Array.isArray(rows)) {
    return { rows, rowCount: rows.length };
  }
  return { rows: [], rowCount: rows.affectedRows || 0 };
}

async function initSchema() {
  await query(`
    CREATE TABLE IF NOT EXISTS users (
      id VARCHAR(36) PRIMARY KEY,
      role VARCHAR(20) NOT NULL,
      email VARCHAR(255) NOT NULL UNIQUE,
      password_hash VARCHAR(255) NOT NULL,
      avatar_url TEXT NULL,
      status VARCHAR(20) NOT NULL DEFAULT 'active',
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS admins (
      user_id VARCHAR(36) PRIMARY KEY,
      full_name VARCHAR(255) NOT NULL,
      position VARCHAR(255) NOT NULL DEFAULT 'Curator',
      status VARCHAR(20) NOT NULL DEFAULT 'active',
      CONSTRAINT fk_admin_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS students (
      user_id VARCHAR(36) PRIMARY KEY,
      curator_id VARCHAR(36) NOT NULL,
      full_name VARCHAR(255) NOT NULL,
      profile_json JSON NULL,
      spirituality_score INT NOT NULL DEFAULT 0,
      language VARCHAR(50) NULL,
      is_starosta TINYINT(1) NOT NULL DEFAULT 0,
      starosta_assigned_at DATETIME NULL,
      starosta_assigned_by VARCHAR(36) NULL,
      CONSTRAINT fk_student_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
      CONSTRAINT fk_student_curator FOREIGN KEY (curator_id) REFERENCES users(id),
      CONSTRAINT fk_student_starosta_by FOREIGN KEY (starosta_assigned_by) REFERENCES users(id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS otp_verifications (
      id VARCHAR(36) PRIMARY KEY,
      email VARCHAR(255) NOT NULL,
      code VARCHAR(10) NOT NULL,
      expires_at DATETIME NOT NULL,
      verified_at DATETIME NULL,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS news (
      id VARCHAR(36) PRIMARY KEY,
      author_id VARCHAR(36) NULL,
      title VARCHAR(255) NOT NULL,
      content TEXT NULL,
      type VARCHAR(100) NULL,
      media_url TEXT NULL,
      status VARCHAR(50) NOT NULL DEFAULT 'draft',
      published_at DATETIME NULL,
      CONSTRAINT fk_news_author FOREIGN KEY (author_id) REFERENCES users(id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS tasks (
      id VARCHAR(36) PRIMARY KEY,
      curator_id VARCHAR(36) NOT NULL,
      title VARCHAR(255) NOT NULL,
      description TEXT NULL,
      type VARCHAR(100) NULL,
      category VARCHAR(255) NOT NULL DEFAULT 'Ma''naviyat va milliy qadriyatlar',
      attachment_url TEXT NULL,
      deadline_at DATETIME NULL,
      status VARCHAR(50) NOT NULL DEFAULT 'active',
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      CONSTRAINT fk_tasks_curator FOREIGN KEY (curator_id) REFERENCES users(id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS task_assignments (
      id VARCHAR(36) PRIMARY KEY,
      task_id VARCHAR(36) NOT NULL,
      student_id VARCHAR(36) NOT NULL,
      status VARCHAR(50) NOT NULL DEFAULT 'under_review',
      graded_score INT NULL,
      points_applied INT NOT NULL DEFAULT 0,
      feedback TEXT NULL,
      updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uk_task_student (task_id, student_id),
      CONSTRAINT fk_assignment_task FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
      CONSTRAINT fk_assignment_student FOREIGN KEY (student_id) REFERENCES users(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS task_submissions (
      id VARCHAR(36) PRIMARY KEY,
      assignment_id VARCHAR(36) NOT NULL,
      file_url TEXT NULL,
      text TEXT NULL,
      submitted_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      CONSTRAINT fk_submission_assignment FOREIGN KEY (assignment_id) REFERENCES task_assignments(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS events (
      id VARCHAR(36) PRIMARY KEY,
      curator_id VARCHAR(36) NOT NULL,
      title VARCHAR(255) NOT NULL,
      description TEXT NULL,
      start_at DATETIME NULL,
      end_at DATETIME NULL,
      type VARCHAR(100) NULL,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      CONSTRAINT fk_events_curator FOREIGN KEY (curator_id) REFERENCES users(id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS event_participants (
      id VARCHAR(36) PRIMARY KEY,
      event_id VARCHAR(36) NOT NULL,
      student_id VARCHAR(36) NOT NULL,
      status VARCHAR(50) NOT NULL DEFAULT 'joined',
      joined_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uk_event_student (event_id, student_id),
      CONSTRAINT fk_participant_event FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE,
      CONSTRAINT fk_participant_student FOREIGN KEY (student_id) REFERENCES users(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS discussions (
      id VARCHAR(36) PRIMARY KEY,
      title VARCHAR(255) NOT NULL,
      created_by VARCHAR(36) NOT NULL,
      scope VARCHAR(255) NULL,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      CONSTRAINT fk_discussion_user FOREIGN KEY (created_by) REFERENCES users(id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS discussion_posts (
      id VARCHAR(36) PRIMARY KEY,
      discussion_id VARCHAR(36) NOT NULL,
      user_id VARCHAR(36) NOT NULL,
      content TEXT NOT NULL,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      CONSTRAINT fk_post_discussion FOREIGN KEY (discussion_id) REFERENCES discussions(id) ON DELETE CASCADE,
      CONSTRAINT fk_post_user FOREIGN KEY (user_id) REFERENCES users(id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS notifications (
      id VARCHAR(36) PRIMARY KEY,
      user_id VARCHAR(36) NOT NULL,
      type VARCHAR(50) NULL,
      title VARCHAR(255) NULL,
      body TEXT NULL,
      read_at DATETIME NULL,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      CONSTRAINT fk_notification_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS student_points_ledger (
      id VARCHAR(36) PRIMARY KEY,
      student_id VARCHAR(36) NOT NULL,
      source_type VARCHAR(50) NOT NULL,
      source_id VARCHAR(36) NULL,
      points INT NOT NULL,
      note TEXT NULL,
      created_by VARCHAR(36) NULL,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      CONSTRAINT fk_points_student FOREIGN KEY (student_id) REFERENCES users(id) ON DELETE CASCADE,
      CONSTRAINT fk_points_created_by FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS student_badges (
      id VARCHAR(36) PRIMARY KEY,
      student_id VARCHAR(36) NOT NULL,
      badge_code VARCHAR(100) NOT NULL,
      badge_name VARCHAR(255) NOT NULL,
      badge_icon VARCHAR(100) NULL,
      badge_description TEXT NULL,
      awarded_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uk_student_badge (student_id, badge_code),
      CONSTRAINT fk_badge_student FOREIGN KEY (student_id) REFERENCES users(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS weekly_challenges (
      id VARCHAR(36) PRIMARY KEY,
      created_by VARCHAR(36) NOT NULL,
      title VARCHAR(255) NOT NULL,
      description TEXT NULL,
      category VARCHAR(255) NULL,
      mode VARCHAR(20) NOT NULL DEFAULT 'solo',
      reward_text TEXT NULL,
      bonus_points INT NOT NULL DEFAULT 0,
      starts_at DATETIME NOT NULL,
      ends_at DATETIME NOT NULL,
      status VARCHAR(20) NOT NULL DEFAULT 'active',
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      CONSTRAINT fk_weekly_challenge_creator FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS weekly_challenge_entries (
      id VARCHAR(36) PRIMARY KEY,
      challenge_id VARCHAR(36) NOT NULL,
      student_id VARCHAR(36) NOT NULL,
      text TEXT NULL,
      file_url TEXT NULL,
      group_name VARCHAR(255) NULL,
      opponent_student_id VARCHAR(36) NULL,
      status VARCHAR(20) NOT NULL DEFAULT 'submitted',
      score INT NULL,
      rank_position INT NULL,
      feedback TEXT NULL,
      points_applied INT NOT NULL DEFAULT 0,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY uk_challenge_student (challenge_id, student_id),
      CONSTRAINT fk_challenge_entry_challenge FOREIGN KEY (challenge_id) REFERENCES weekly_challenges(id) ON DELETE CASCADE,
      CONSTRAINT fk_challenge_entry_student FOREIGN KEY (student_id) REFERENCES users(id) ON DELETE CASCADE,
      CONSTRAINT fk_challenge_entry_opponent FOREIGN KEY (opponent_student_id) REFERENCES users(id) ON DELETE SET NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS challenge_entry_likes (
      id VARCHAR(36) PRIMARY KEY,
      entry_id VARCHAR(36) NOT NULL,
      user_id VARCHAR(36) NOT NULL,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uk_challenge_entry_like (entry_id, user_id),
      CONSTRAINT fk_challenge_like_entry FOREIGN KEY (entry_id) REFERENCES weekly_challenge_entries(id) ON DELETE CASCADE,
      CONSTRAINT fk_challenge_like_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS challenge_entry_comments (
      id VARCHAR(36) PRIMARY KEY,
      entry_id VARCHAR(36) NOT NULL,
      user_id VARCHAR(36) NOT NULL,
      text TEXT NOT NULL,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      CONSTRAINT fk_challenge_comment_entry FOREIGN KEY (entry_id) REFERENCES weekly_challenge_entries(id) ON DELETE CASCADE,
      CONSTRAINT fk_challenge_comment_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS certificates (
      id VARCHAR(36) PRIMARY KEY,
      student_id VARCHAR(36) NOT NULL,
      challenge_id VARCHAR(36) NULL,
      issued_by VARCHAR(36) NOT NULL,
      title VARCHAR(255) NOT NULL,
      rank_label VARCHAR(50) NULL,
      award_type VARCHAR(50) NULL,
      award_month_key VARCHAR(7) NULL,
      award_rank INT NULL,
      note TEXT NULL,
      template_name VARCHAR(255) NULL,
      template_url TEXT NULL,
      pdf_url TEXT NULL,
      issued_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      CONSTRAINT fk_certificate_student FOREIGN KEY (student_id) REFERENCES users(id) ON DELETE CASCADE,
      CONSTRAINT fk_certificate_challenge FOREIGN KEY (challenge_id) REFERENCES weekly_challenges(id) ON DELETE SET NULL,
      CONSTRAINT fk_certificate_issuer FOREIGN KEY (issued_by) REFERENCES users(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS monthly_leaderboard (
      id VARCHAR(36) PRIMARY KEY,
      month_key VARCHAR(7) NOT NULL,
      student_id VARCHAR(36) NOT NULL,
      rank_position INT NOT NULL,
      score_value INT NOT NULL DEFAULT 0,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uk_month_rank (month_key, rank_position),
      UNIQUE KEY uk_month_student (month_key, student_id),
      CONSTRAINT fk_monthly_leaderboard_student FOREIGN KEY (student_id) REFERENCES users(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS monthly_award_runs (
      id VARCHAR(36) PRIMARY KEY,
      month_key VARCHAR(7) NOT NULL UNIQUE,
      status VARCHAR(20) NOT NULL DEFAULT 'processing',
      message TEXT NULL,
      processed_at DATETIME NULL,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS analytics_snapshots (
      id VARCHAR(36) PRIMARY KEY,
      scope VARCHAR(255) NULL,
      data_json JSON NULL,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS news_likes (
      id VARCHAR(36) PRIMARY KEY,
      news_id VARCHAR(36) NOT NULL,
      user_id VARCHAR(36) NOT NULL,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uk_news_like (news_id, user_id),
      CONSTRAINT fk_like_news FOREIGN KEY (news_id) REFERENCES news(id) ON DELETE CASCADE,
      CONSTRAINT fk_like_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS news_comments (
      id VARCHAR(36) PRIMARY KEY,
      news_id VARCHAR(36) NOT NULL,
      user_id VARCHAR(36) NOT NULL,
      text TEXT NOT NULL,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      CONSTRAINT fk_comment_news FOREIGN KEY (news_id) REFERENCES news(id) ON DELETE CASCADE,
      CONSTRAINT fk_comment_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS attendances (
      id VARCHAR(36) PRIMARY KEY,
      curator_id VARCHAR(36) NOT NULL,
      starosta_id VARCHAR(36) NOT NULL,
      student_id VARCHAR(36) NOT NULL,
      date_key VARCHAR(10) NOT NULL,
      present TINYINT(1) NOT NULL DEFAULT 0,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY uk_attendance_student_date (student_id, date_key),
      CONSTRAINT fk_attendance_curator FOREIGN KEY (curator_id) REFERENCES users(id),
      CONSTRAINT fk_attendance_starosta FOREIGN KEY (starosta_id) REFERENCES users(id),
      CONSTRAINT fk_attendance_student FOREIGN KEY (student_id) REFERENCES users(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  try { await query('CREATE INDEX idx_students_curator_id ON students(curator_id)'); } catch (_) {}
  try { await query('CREATE INDEX idx_tasks_curator_id ON tasks(curator_id)'); } catch (_) {}
  try { await query('CREATE INDEX idx_task_assignments_task_id ON task_assignments(task_id)'); } catch (_) {}
  try { await query('CREATE INDEX idx_task_assignments_student_id ON task_assignments(student_id)'); } catch (_) {}
  try { await query('CREATE INDEX idx_events_curator_id ON events(curator_id)'); } catch (_) {}
  try { await query('CREATE INDEX idx_attendances_curator_date ON attendances(curator_id, date_key)'); } catch (_) {}
  try { await query('ALTER TABLE task_assignments ADD COLUMN points_applied INT NOT NULL DEFAULT 0'); } catch (_) {}
  try { await query('CREATE INDEX idx_points_ledger_student_date ON student_points_ledger(student_id, created_at)'); } catch (_) {}
  try { await query('CREATE INDEX idx_badges_student ON student_badges(student_id)'); } catch (_) {}
  try { await query('CREATE INDEX idx_weekly_challenges_dates ON weekly_challenges(starts_at, ends_at)'); } catch (_) {}
  try { await query('CREATE INDEX idx_challenge_entries_challenge ON weekly_challenge_entries(challenge_id)'); } catch (_) {}
  try { await query('CREATE INDEX idx_challenge_entries_student ON weekly_challenge_entries(student_id)'); } catch (_) {}
  try { await query('CREATE INDEX idx_challenge_comments_entry ON challenge_entry_comments(entry_id)'); } catch (_) {}
  try { await query('CREATE INDEX idx_certificates_student ON certificates(student_id, issued_at)'); } catch (_) {}
  try { await query('CREATE INDEX idx_certificates_award_month ON certificates(award_month_key, award_type, award_rank)'); } catch (_) {}
  try { await query('CREATE INDEX idx_monthly_leaderboard_month ON monthly_leaderboard(month_key, rank_position)'); } catch (_) {}
  try { await query('CREATE INDEX idx_monthly_leaderboard_student ON monthly_leaderboard(student_id, month_key)'); } catch (_) {}
  try { await query('ALTER TABLE certificates ADD COLUMN award_type VARCHAR(50) NULL'); } catch (_) {}
  try { await query('ALTER TABLE certificates ADD COLUMN award_month_key VARCHAR(7) NULL'); } catch (_) {}
  try { await query('ALTER TABLE certificates ADD COLUMN award_rank INT NULL'); } catch (_) {}
}

async function connectDb() {
  const mysqlUrl = process.env.MYSQL_URL || process.env.DATABASE_URL;
  if (mysqlUrl) {
    pool = mysql.createPool(mysqlUrl);
  } else {
    pool = mysql.createPool({
      host: process.env.MYSQL_HOST || '127.0.0.1',
      port: Number(process.env.MYSQL_PORT || 3306),
      user: process.env.MYSQL_USER || 'root',
      password: process.env.MYSQL_PASSWORD || '',
      database: process.env.MYSQL_DATABASE || 'talaba_tarbiya',
      waitForConnections: true,
      connectionLimit: 10,
      queueLimit: 0,
    });
  }

  await query('SELECT 1');
  await initSchema();
}

async function ensureSuperAdmin() {
  const email = process.env.SUPER_ADMIN_EMAIL;
  const password = process.env.SUPER_ADMIN_PASSWORD;

  if (!email || !password) {
    return;
  }

  const normalizedEmail = email.trim().toLowerCase();
  const existing = await query('SELECT id FROM users WHERE email = $1 LIMIT 1', [normalizedEmail]);
  if (existing.rowCount > 0) {
    return;
  }

  const userId = createId();
  const passwordHash = await bcrypt.hash(password, 10);
  await query(
    `INSERT INTO users (id, role, email, password_hash)
     VALUES ($1, $2, $3, $4)`,
    [userId, 'super', normalizedEmail, passwordHash],
  );

  await query(
    `INSERT INTO admins (user_id, full_name, position, status)
     VALUES ($1, $2, $3, $4)`,
    [userId, 'Super Admin', 'System Owner', 'active'],
  );
}

module.exports = {
  connectDb,
  ensureSuperAdmin,
  query,
  createId,
};
