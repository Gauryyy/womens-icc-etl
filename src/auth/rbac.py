from src.auth.models import get_connection

print("RBAC FILE LOADED")  # debug

def get_user_permissions(user_id):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT p.permission_name
    FROM users u
    JOIN roles r ON u.role_id = r.id
    JOIN role_permissions rp ON r.id = rp.role_id
    JOIN permissions p ON rp.permission_id = p.id
    WHERE u.id = ?
    """, (user_id,))

    permissions = [row[0] for row in cursor.fetchall()]
    conn.close()
    return permissions


def has_permission(user_id, permission):
    return permission in get_user_permissions(user_id)