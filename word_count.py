from sys import stderr
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w']+")


class MRMostUsedWord(MRJob):

    def mapper_get_words(self, _, line):
        # yield each word in the line
        for word in line.split():
            yield word, 1 

    def reducer_count_words(self, word, counts):
        # stderr.write("GENERATOR FOR WORD >> {0}\n".format(word))
        # for c in counts:
        #     stderr.write("REDUCER >>  counts {0}\n".format(c))

        # stderr.write("REDUCER >> word {0}, counts {1}\n".format(word, counts))
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        yield None, (sum(counts), word)

    # discard the key; it is just None
    def reducer_find_max_word(self, _, word_count_pairs):
        # each item of word_count_pairs is (count, word),
        # so yielding one results in key=counts, value=word
        yield max(word_count_pairs)

    def my_custom_mapper(self, _, value):
        stderr.write("MAPPER >> value {0}\n".format(type(value)))
        yield 'value', value

    def my_custom_reducer(self, _, value):
        # stderr.write("REDUCER >> value {0}\n".format(value))
        # for v in value:
        #     stderr.write("REDUCER >> value {0}\n".format(v))
        yield (max(value), "hello!")

    def steps(self):
        # return [
        #     MRStep(mapper=self.mapper_get_words,
        #         #    combiner=self.combiner_count_words,
        #            reducer=self.reducer_count_words),
        #     MRStep(reducer=self.reducer_find_max_word)
        # ]
        return [
            MRStep(mapper=self.mapper_get_words, 
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.my_custom_reducer)
        ]

if __name__ == '__main__':
    MRMostUsedWord.run()