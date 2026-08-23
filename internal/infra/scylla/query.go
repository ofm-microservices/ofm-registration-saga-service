package scylla

const (
	insertSessionQuery = `
		INSERT INTO registration_sessions (session_id, client_id, user_id, email, username, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`

	getSessionByIDQuery = `
		SELECT session_id, client_id, user_id, email, username, status, created_at, updated_at
		FROM registration_sessions
		WHERE session_id = ?
		LIMIT 1
	`

	getSessionByEmailQuery = `
		SELECT session_id, client_id, user_id, email, username, status, created_at, updated_at
		FROM registration_sessions_by_email
		WHERE email = ?
		LIMIT 1
	`

	getSessionByUsernameQuery = `
		SELECT session_id, client_id, user_id, email, username, status, created_at, updated_at
		FROM registration_sessions_by_username
		WHERE username = ?
		LIMIT 1
	`

	insertSessionByEmailQuery = `
		INSERT INTO registration_sessions_by_email (email, session_id, client_id, user_id, username, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`

	insertSessionByUsernameQuery = `
		INSERT INTO registration_sessions_by_username (username, session_id, client_id, user_id, email, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`

	updateSessionStatusQuery = `
		UPDATE registration_sessions
		SET status = ?, updated_at = ?
		WHERE session_id = ?
	`

	updateSessionByEmailStatusQuery = `
		UPDATE registration_sessions_by_email
		SET status = ?, updated_at = ?
		WHERE email = ?
	`

	updateSessionByUsernameStatusQuery = `
		UPDATE registration_sessions_by_username
		SET status = ?, updated_at = ?
		WHERE username = ?
	`

	claimCompletedSessionQuery = `
		UPDATE registration_sessions
		SET status = ?, updated_at = ?
		WHERE session_id = ?
		IF status = ?
	`

	insertStepQuery = `
		INSERT INTO registration_steps (session_id, step_key, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?)
	`

	getStepByKeyQuery = `
		SELECT session_id, step_key, status, created_at, updated_at
		FROM registration_steps
		WHERE session_id = ? AND step_key = ?
		LIMIT 1
	`

	listStepsBySessionIDQuery = `
		SELECT session_id, step_key, status, created_at, updated_at
		FROM registration_steps
		WHERE session_id = ?
	`

	updateStepStatusQuery = `
		UPDATE registration_steps
		SET status = ?, updated_at = ?
		WHERE session_id = ? AND step_key = ?
	`
	updateStepInProgressQuery = `
		UPDATE registration_steps
		SET status = ?, updated_at = ?
		WHERE session_id = ? AND step_key = ?
		IF status != 'completed'
	`
)
