
# --- Display Formatting ---- #
from header_box import print_header_box


def get_user_choice() -> str:
    """
    Prompt the user to enter a digit representing their choice of action.

    Returns:
        str: The user's input choice.
    """
   
    print_header_box(header_text = 'CUSTOMER DATA MANAGEMENT SYSTEM', box_width = 100)
    
    # Let users know what they can do.
    print("\nPlease enter a digit from [1-7] for the corresponding action:")
    print()
    print("[1] Display the transactions made by customers living in a given zip code for a given month and year.")
    print("[2] Display the number and total values of transactions for a given type.")
    print("[3] Display the total number and total values of transactions for branches in a given state.")
    print("[4] Check the existing account details.")
    print("[5] Modify the existing account details.")
    print("[6] Generate a monthly bill for a credit card number.")
    print("[7] Display the transactions made by a customer between two dates.")
    print("[q] Quit.")

    return input("What would you like to do?")

