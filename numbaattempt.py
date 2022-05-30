import time
from numba import jit


@jit(nopython=True)
def shingles_helper(text: str, char_ngram: int):
    return [text[head:head + char_ngram] for head in range(0, len(text) - char_ngram)]


def shingles(text: str, char_ngram: int):
    return set(text[head:head + char_ngram] for head in range(0, len(text) - char_ngram))
    return set(shingles_helper(text, char_ngram))
    # my_set = list()
    # for head in range(0, len(text) - char_ngram):
    #     my_set.append(text[head:head + char_ngram])
    # return my_set


if __name__ == '__main__':
    start_time = time.time()
    text_str = "Hello there my friend, this is lovely aint it lol haha lmao hehe xD harry potter as always haha" * 3
    for i in range(200000):
        sh = shingles(text_str, 7)
        # sh = set(shingles(text_str, 7))
    print(time.time() - start_time)
