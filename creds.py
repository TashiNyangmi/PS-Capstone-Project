from typing import Tuple

def read_cred_from_file() -> Tuple[str, str]:
    """
    Description:
        Reads the MySQL credentials from a secrets file.
    Usage Example:
        username, password = read_cred_from_file()
    Returns:
        A tuple containing the MySQL username and password.
    """
    try:
        with open('secrets.txt', 'r') as f:
            lines = f.readlines()

        secrets = {}
        for line in lines:
            key, value = line.strip().split('=')
            secrets[key] = value

        db_username = secrets.get('DB_USERNAME')
        db_password = secrets.get('DB_PASSWORD')

        if db_username is None or db_password is None:
            raise ValueError("Missing credentials in secrets file.")

        return db_username, db_password

    except FileNotFoundError:
        raise FileNotFoundError("Secrets file not found.")

    except Exception as e:
        raise Exception("Error occurred while reading credentials: " + str(e))
    

    