from mrjob.job import MRJob

class TagsParUtilisateur(MRJob):

    def mapper(self, _, line):
        try:
            line = line.strip()
            parts = line.split(',')
            userId, movieId, tag = parts[0], parts[1], parts[2]
            if userId == 'userId':
                return
            yield userId, 1
        except Exception:
            pass

    def reducer(self, userId, counts):
        yield userId, sum(counts)

if __name__ == '__main__':
    TagsParUtilisateur.run()
