# CC2 – Hadoop MapReduce : Analyse du fichier `tags.csv` (MovieLens 25M)

**Auteurs :** Andrew DESVALCY, Tristan CHABANEL, Arnaud CORDIER  
**Fichier source :** `ml-25m/tags.csv` — 1 093 361 lignes, 37,01 Mo

---

## Structure du dépôt

```
.
├── DESVALCY_CHABANEL.md                          ← ce rapport

├── tags.csv                          ← fichier source complet (1 093 361 lignes, 37 Mo)

├── tags_per_movie.py                 ← Q1 : tags par film
├── tags_per_user.py                  ← Q2 : tags par utilisateur
├── tag_count.py                      ← Q4 : fréquence des tags
├── tags_per_movie_user.py            ← Q5 : tags par (film, user)

├── output_tags_per_movie.txt         ← résultats Q1 (45 251 lignes)
├── output_tags_per_user.txt          ← résultats Q2 (14 592 lignes)
├── output_tag_count.txt               ← résultats Q4 (65 361 lignes)
└── output_tags_per_movie_user.txt    ← résultats Q5 (305 356 lignes)
```

---

## 1. Préparation et démarche générale

### Structure du fichier source

Le fichier `tags.csv` contient les annotations libres (tags) posées par les utilisateurs du site MovieLens sur des films. Chaque ligne correspond à un tag unique posé par un utilisateur :

```
userId,movieId,tag,timestamp
3,260,classic,1439472355
3,260,sci-fi,1439472256
4,1732,dark comedy,1573943598
```

Deux points importants observés lors de l'inspection du fichier :
- Les fins de ligne sont au format Windows (`\r\n`), ce qui peut provoquer des artefacts dans les clés si on ne fait pas de `.strip()` dans le mapper
- Certaines lignes sont mal formées (colonnes manquantes ou malformées), ce qui impose un bloc `try/except` pour ne pas faire planter le job

### Connexion au cluster Hadoop

La connexion au sandbox HDP se fait via SSH depuis un terminal Windows (PowerShell) :

```bash
ssh maria_dev@127.0.0.1 -p 2222
# mot de passe : maria_dev
```

### Précautions avant de commencer

**1. Vérifier que HDFS est démarré :**

```bash
hdfs dfsadmin -report
```

Si la commande échoue avec `Connection refused`, HDFS n'est pas démarré. Il faut passer par l'interface Ambari sur `http://localhost:8080` (login : `admin` / `admin`), puis aller dans **Services → HDFS → Start**.

**2. Vérifier que Python et mrjob sont disponibles :**

```bash
python --version
pip show mrjob
```

Si mrjob n'est pas installé :

```bash
pip install mrjob --user
```

**3. Transférer les fichiers depuis le PC local vers le cluster :**

Le fichier `tags.csv` et les scripts Python doivent être copiés sur le cluster. Depuis un terminal Windows (pas dans le SSH) :

Depuis un terminal Windows PowerShell (**pas dans le SSH**, ouvrir une nouvelle fenêtre) :

```bash
# Transférer le fichier de données
scp -P 2222 C:\chemin\vers\ton\dossier\tags.csv maria_dev@127.0.0.1:~/

# Transférer les scripts un par un
scp -P 2222 C:\chemin\vers\ton\dossier\tags_per_movie.py maria_dev@127.0.0.1:~/
scp -P 2222 C:\chemin\vers\ton\dossier\tags_per_user.py maria_dev@127.0.0.1:~/
scp -P 2222 C:\chemin\vers\ton\dossier\tag_count.py maria_dev@127.0.0.1:~/
scp -P 2222 C:\chemin\vers\ton\dossier\tags_per_movie_user.py maria_dev@127.0.0.1:~/
```

> Remplace `C:\chemin\vers\ton\dossier\` par le chemin réel où tu as mis les fichiers, par exemple `C:\Users\adesvalcy\Downloads\hadoop-cc2\`. Le mot de passe demandé à chaque fois est `maria_dev`.

Vérifier que tous les fichiers sont bien arrivés sur le cluster (dans le terminal SSH) :

```bash
ls ~/ | grep -E ".py|.csv"
```

### Chargement dans HDFS

```bash
# Configuration par défaut (taille de bloc = 128 Mo)
hdfs dfs -put tags.csv /user/maria_dev/tags.csv

# Configuration personnalisée (taille de bloc = 64 Mo)
hdfs dfs -D dfs.blocksize=67108864 -put tags.csv /user/maria_dev/tags_64mb.csv
```

---

## 2. Configuration par défaut de Hadoop

### Question 1 — Nombre de tags par film

**Démarche :** Pour chaque ligne valide, le mapper émet `(movieId, 1)`. Le reducer reçoit tous les 1 associés à un même film et les additionne. C'est l'équivalent d'un `GROUP BY movieId COUNT(*)` en SQL.

**Script [`tags_per_movie.py`](https://github.com/andrewdesvalcy/hadoop-cc2/blob/main/tags_per_movie.py) :**

```python
from mrjob.job import MRJob

class TagsParFilm(MRJob):

    def mapper(self, _, line):
        try:
            line = line.strip()
            parts = line.split(',')
            userId, movieId, tag = parts[0], parts[1], parts[2]
            if movieId == 'movieId':  # ignorer l'en-tête
                return
            yield movieId, 1
        except Exception:
            pass

    def reducer(self, movieId, counts):
        yield movieId, sum(counts)

if __name__ == '__main__':
    TagsParFilm.run()
```

**Lancement Hadoop :**

```bash
python tags_per_movie.py -r hadoop \
  --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
  hdfs:///user/maria_dev/tags.csv \
  -o hdfs:///user/maria_dev/output_tags_per_movie
```

**Résultats — 45 251 films distincts. Top 30 films les plus tagués :**

```
"260"       6180
"79132"     4767
"296"       4767
"109487"    3616
"2959"      3612
"318"       3597
"2571"      3573
"356"       2701
"4226"      2601
"7361"      2533
"593"       2482
"4878"      2417
"4973"      2226
"48780"     2126
"115713"    2115
"72998"     2105
"541"       2081
"58559"     1956
"47"        1947
"2858"      1904
"68237"     1875
"74458"     1862
"164179"    1788
"44191"     1778
"5618"      1769
"924"       1749
"68157"     1657
"1206"      1645
"122882"    1643
"3949"      1637
```

**[→ Résultats complets](https://github.com/andrewdesvalcy/hadoop-cc2/blob/main/output_tags_per_movie.txt)**

**Analyse :** Le film `260` (*Star Wars: Episode IV — A New Hope*) arrive en tête avec 6 180 tags, ce qui reflète sa popularité dans la communauté MovieLens. Sur les 45 251 films présents dans le fichier, 6 874 n'ont reçu qu'un seul tag, et la moyenne est de 24,2 tags par film. La distribution est donc très asymétrique : quelques blockbusters concentrent l'essentiel des annotations.

---

### Question 2 — Nombre de tags ajoutés par utilisateur

**Démarche :** Même logique que la Q1, en changeant uniquement la clé d'émission du mapper : on émet `(userId, 1)` au lieu de `(movieId, 1)`. Chaque reducer agrège le total de tags posés par un même utilisateur.

**Script [`tags_per_user.py`](https://github.com/andrewdesvalcy/hadoop-cc2/blob/main/tags_per_user.py) :**

```python
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
```

**Lancement Hadoop :**

```bash
python tags_per_user.py -r hadoop \
  --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
  hdfs:///user/maria_dev/tags.csv \
  -o hdfs:///user/maria_dev/output_tags_per_user
```

**Résultats — 14 592 utilisateurs distincts. Top 30 utilisateurs les plus actifs :**

```
"6550"      183356
"21096"     20317
"62199"     13700
"160540"    12076
"155146"    11445
"70092"     10582
"131347"    10195
"14116"     10167
"31047"     8463
"141263"    7114
"64333"     6944
"47969"     6599
"15204"     6426
"84824"     6209
"123527"    6005
"148457"    5960
"19346"     5919
"6285"      5837
"96795"     5334
"44444"     4941
"56238"     4632
"151732"    4429
"78897"     4327
"3448"      4256
"149463"    4146
"34047"     4085
"126013"    4050
"33844"     4029
"15078"     4011
"141361"    3922
```

**[→ Résultats complets](https://github.com/andrewdesvalcy/hadoop-cc2/blob/main/output_tags_per_user.txt)**

**Analyse :** L'utilisateur `6550` est massivement au-dessus des autres avec 183 356 tags — soit environ 17% de l'ensemble du fichier à lui seul. La médiane est de 5 tags par utilisateur, et 9 417 utilisateurs sur 14 592 en ont posé moins de 10. On est typiquement face à une loi de Pareto : une minorité d'utilisateurs très actifs produit la grande majorité du contenu.

---

## 3. Configuration Hadoop avec taille de bloc = 64 Mo

### Question 3 — Nombre de blocs occupés dans HDFS

Pour connaître le découpage en blocs du fichier dans chacune des deux configurations, on interroge HDFS avec `fsck` :

```bash
# Config par défaut (128 Mo)
hdfs fsck /user/maria_dev/tags.csv -files -blocks

# Config personnalisée (64 Mo)
hdfs fsck /user/maria_dev/tags_64mb.csv -files -blocks
```

Le fichier `tags.csv` pèse exactement **38 810 332 octets (37,01 Mo)**. Étant inférieur aux deux seuils de bloc, il tient dans un unique bloc dans les deux configurations :

| Configuration | Taille du bloc | Taille du fichier | Nombre de blocs |
|---|---|---|---|
| Par défaut | 128 Mo | 37,01 Mo | **1** |
| Personnalisée | 64 Mo | 37,01 Mo | **1** |

**Analyse :** Le résultat est identique car `37 Mo < 64 Mo < 128 Mo`. Changer la taille de bloc n'a ici aucun impact sur le parallélisme — Hadoop ne lance qu'un seul mapper dans les deux cas. Si le fichier avait dépassé 64 Mo (par exemple avec `ml-25m/ratings.csv` qui fait plusieurs centaines de Mo), la configuration à 64 Mo aurait découpé le fichier en plusieurs blocs et permis de lancer autant de mappers en parallèle, réduisant le temps de traitement.

---

### Question 4 — Nombre d'utilisations de chaque tag

**Démarche :** Cette fois, la clé du mapper est le tag lui-même. On normalise avec `.strip().lower()` pour regrouper les variantes de casse (`Sci-Fi` et `sci-fi` doivent compter comme le même tag). Le reducer somme les occurrences par tag.

**Script [`tag_count.py`](https://github.com/andrewdesvalcy/hadoop-cc2/blob/main/tag_count.py) :**

```python
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
```

**Lancement Hadoop :**

```bash
python tag_count.py -r hadoop \
  --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
  hdfs:///user/maria_dev/tags_64mb.csv \
  -o hdfs:///user/maria_dev/output_tag_count
```

**Résultats — 65 361 tags distincts. Top 30 tags les plus utilisés :**

```
"sci-fi"              8795
"atmospheric"         7053
"action"              6783
"comedy"              6368
"surreal"             5584
"funny"               5354
"based on a book"     5194
"twist ending"        4904
"visually appealing"  4691
"romance"             4482
"dystopia"            4329
"dark comedy"         4133
"quirky"              3999
"bd-r"                3966
"thought-provoking"   3847
"fantasy"             3816
"stylized"            3758
"classic"             3733
"psychology"          3646
"nudity (topless)"    3590
"woman director"      3590
"violence"            3386
"time travel"         3306
"murder"              3302
"adventure"           3287
"social commentary"   3251
"dark"                3248
"thriller"            3247
"revenge"             3146
"drama"               3135
```

**[→ Résultats complets](https://github.com/andrewdesvalcy/hadoop-cc2/blob/main/output_tag_count.txt)**

**Analyse :** Sur les 65 361 tags distincts recensés, 31 696 (soit 48%) n'ont été utilisés qu'une seule fois — ce sont des tags très spécifiques ou des fautes de frappe. À l'inverse, seulement 156 tags dépassent les 1 000 utilisations. `"sci-fi"` domine avec 8 795 occurrences, ce qui est cohérent avec la composition du catalogue MovieLens, très orienté science-fiction et fantastique. La présence de `"bd-r"` (format Blu-ray) parmi les tops indique que certains utilisateurs annotent aussi les caractéristiques techniques du support, pas seulement le contenu.

---

### Question 5 — Tags introduits par le même utilisateur pour chaque film

**Démarche :** On veut savoir, pour chaque paire (film, utilisateur), combien de tags différents cet utilisateur a posés sur ce film. La clé composite doit être `(movieId, userId)`. On sérialise cette paire en JSON pour éviter tout conflit de parsing (un simple `movieId + "_" + userId` serait risqué si les IDs contenaient ce caractère). Le reducer comptabilise les 1 reçus pour chaque paire.

**Script [`tags_per_movie_user.py`](https://github.com/andrewdesvalcy/hadoop-cc2/blob/main/tags_per_movie_user.py) :**

```python
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
```

**Lancement Hadoop :**

```bash
python tags_per_movie_user.py -r hadoop \
  --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
  hdfs:///user/maria_dev/tags_64mb.csv \
  -o hdfs:///user/maria_dev/output_tags_per_movie_user
```

**Résultats — 305 356 paires (film, user) distinctes. Top 30 :**

```
"film=318, user=31047"      337
"film=1103, user=31047"     250
"film=296, user=131341"     173
"film=296, user=155146"     153
"film=1527, user=31047"     130
"film=260, user=27598"      127
"film=318, user=162440"     106
"film=72998, user=19346"    97
"film=296, user=20723"      94
"film=296, user=36159"      94
"film=527, user=31047"      88
"film=79132, user=113919"   88
"film=79132, user=49134"    88
"film=67255, user=82788"    85
"film=79132, user=19346"    84
"film=318, user=137655"     83
"film=109487, user=120385"  80
"film=3399, user=6550"      79
"film=79132, user=120385"   76
"film=2571, user=155146"    74
"film=1214, user=155146"    74
"film=122912, user=20917"   74
"film=7153, user=44444"     74
"film=4973, user=133274"    73
"film=318, user=15204"      72
"film=166528, user=113919"  71
"film=79132, user=68096"    71
"film=122912, user=130233"  70
"film=109487, user=17213"   69
"film=72998, user=155146"   69
```

**[→ Résultats complets](https://github.com/andrewdesvalcy/hadoop-cc2/blob/main/output_tags_per_movie_user.txt)**

**Analyse :** L'utilisateur `31047` a tagué le film `318` (*The Shawshank Redemption*) avec 337 tags distincts. On retrouve ce même utilisateur pour plusieurs films dans le top, ce qui est cohérent avec les 8 463 tags qu'il a posés au total (Q2). Le film `296` (*Pulp Fiction*) revient lui aussi plusieurs fois dans le top, avec différents utilisateurs très actifs. Ce résultat croise et confirme les observations des Q1 et Q2 : les mêmes films populaires et les mêmes utilisateurs hyperactifs se retrouvent systématiquement en tête.

---

## 4. Récupération des résultats depuis HDFS

Une fois les jobs terminés, les fichiers de sortie sont récupérés en local avec `getmerge`, qui fusionne les éventuels fichiers partiels (`part-00000`, `part-00001`, etc.) produits par les reducers :

```bash
hdfs dfs -getmerge hdfs:///user/maria_dev/output_tags_per_movie      results/output_tags_per_movie.txt
hdfs dfs -getmerge hdfs:///user/maria_dev/output_tags_per_user       results/output_tags_per_user.txt
hdfs dfs -getmerge hdfs:///user/maria_dev/output_tag_count           results/output_tag_count.txt
hdfs dfs -getmerge hdfs:///user/maria_dev/output_tags_per_movie_user results/output_tags_per_movie_user.txt
```
