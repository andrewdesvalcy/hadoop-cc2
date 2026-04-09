from mrjob.job import MRJob

class ComptageTag(MRJob):

    def mapper(self, _, line):
        try:
            line = line.strip()
            parts = line.split(',')
            userId, movieId, tag = parts[0], parts[1], parts[2]
            if tag == 'tag':
                return
            yield tag.strip().lower(), 1
        except Exception:
            pass

    def reducer(self, tag, counts):
        yield tag, sum(counts)

if __name__ == '__main__':
    ComptageTag.run()
