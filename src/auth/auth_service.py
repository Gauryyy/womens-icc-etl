from src.auth.models import get_connection

def create_user(username, password, role_name="user"):
    conn = get_connection()
    cursor = conn.cursor()

    # Check if user exists
    cursor.execute("SELECT * FROM users WHERE username=?", (username,))
    if cursor.fetchone():
        return  # already exists

    cursor.execute("SELECT id FROM roles WHERE role_name=?", (role_name,))
    role_id = cursor.fetchone()[0]

    cursor.execute("""
    INSERT INTO users (username, password, role_id)
    VALUES (?, ?, ?)
    """, (username, password, role_id))

    conn.commit()
    conn.close()