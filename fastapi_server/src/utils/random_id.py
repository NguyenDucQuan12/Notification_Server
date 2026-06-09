import random
import string

def get_random_string(length):
    """
    Tạo ngẫu nhiên một chuỗi với độ dài cung cấp
    """
    characters = string.ascii_letters + string.digits + string.punctuation
    random_letter = ''.join(random.choice(characters) for i in range(length))
    # print("Random string of length", length, "is:", random_letter)

    return random_letter


if __name__ == "__main__":
    get_random_string(9)