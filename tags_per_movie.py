from mrjob.job import MRJob

class TagsParFilm(MRJob):

    def mapper(self, _, line):
        try:
            line = line.strip()
            parts = line.split(',')
            userId, movieId, tag = parts[0], parts[1], parts[2]
            if movieId == 'movieId':
                return
            yield movieId, 1
        except Exception:
            pass

    def reducer(self, movieId, counts):
        yield movieId, sum(counts)

if __name__ == '__main__':
    TagsParFilm.run()
