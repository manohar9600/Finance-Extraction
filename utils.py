from collections import Counter

def most_occuring_element(lst):
    if len(lst) == 0:
        return -1, -1
    # Count occurrences of each element in the list
    count_dict = Counter(lst)

    # Find the element with the maximum count
    most_common_element = count_dict.most_common(1)[0][0]
    most_common_count = count_dict.most_common(1)[0][1]

    return most_common_element, most_common_count
