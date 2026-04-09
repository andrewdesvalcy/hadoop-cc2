from mrjob.job import MRJob
import json

class TagsParFilmEtUtilisateur(MRJob):

    def mapper(self, _, line):
        try:
            line = line.strip()
            parts = line.split(',')
            userId, movieId, tag = parts[0], parts[1], parts[2]
            if userId == 'userId':
                return
            yield json.dumps([movieId, userId]), 1
        except Exception:
            pass

    def reducer(self, key, counts):
        movieId, userId = json.loads(key)
        yield "film={}, user={}".format(movieId, userId), sum(counts)

if __name__ == '__main__':
    TagsParFilmEtUtilisateur.run()
