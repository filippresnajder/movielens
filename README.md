# **ETL proces datasetu MovieLens**
Tento repozitár obsahuje implementáciu ETL procesu v Snowflake pre analýzu dát z Movielens datasetu. Projekt sa zameriava na preskúmanie správania používateľov a ich preferencií na základe hodnotení filmov a demografických údajov používateľov. Výsledný dátový model umožňuje multidimenzionálnu analýzu a vizualizáciu kľúčových metrik.

---
# **1. Úvod a popis zdrojových dát**
Cieľom semestrálneho projektu je analyzovať dáta týkajúce sa filmov, používateľov a ich hodnotení. Táto analýza umožňuje identifikovať trendy v filmových preferenciách, najpopulárnejšie filmy a správanie používateľov.

Zdrojové dáta pochádzajú z Grouplens datasetu dostupného [tu](https://grouplens.org/datasets/movielens/). Dataset obsahuje osem hlavných tabuliek:
- `users`
- `ratings`
- `tags`
- `movies`
- `age_group`
- `occupations`
- `genres_movies`
- `genres`

Účelom ETL procesu bolo tieto dáta pripraviť, transformovať a sprístupniť pre viacdimenzionálnu analýzu.

---
### **1.1 Dátová architektúra**

Tabuľka `users` obsahuje atribúty:
- id, typu INT, ktorý je primárny kľúč
- age, typu INT, ktorý je cudzím kľučom tabuľky age_group
- gender typu CHAR, ktorý drží 1 znak označujúci pohlavie uživateľa (M - Male / F - Female)
- zip_code typu VARCHAR, ktorý drží poštové smerovacie číslo užívateľa
- occupation_id typu INT, ktorý je cudzím kľúčom tabuľky occupations

Tabuľka `ratings` obsahuje atribúty:
- id, typu INT, ktorý je primárny kľúč
- user_id, typu INT, ktorý je cudzím kľúčom tabuľy users
- movie_id, typu INT, ktorý je cudzím kľúčom tabuľky movies
- rating, typu INT, ktorý ukľadá číselnú hodnotu (od 1 po 5), ktorá predstavuje hodnotenie daného filmu
- rated_at, typu DATETIME, ktorý ukladá údaj o dni a čase, kedy bolo hodnotenie uskutočnené

Tabuľka `tags` je spojovacia tabuľka pre tabuľku `users` a `movies`, ktorá obsahuje atribúty:
- id, typu INT, ktorý je primárny kľúč
- user_id, typu INT, ktorý je cudzím kľúčom tabuľy users
- movie_id, typu INT, ktorý je cudzím kľúčom tabuľky movies
- tags, typue VARCHAR, ktorý ukladá jednotlivé tagy, ktoré uživatelia zanechali k danému filmu
- created_at, typu DATETIME, ktorý ukladá údaj o dni a čase, kedy bol tag pridaný

Tabuľka `movies` obsahuje atribúty:
- id, typu INT, ktorý je primárny kľúč
- title, typu VARCHAR, ktorý ukladá názov daného filmu
- release_year, typu CHAR, ktorý ukladá rok, v ktorom daný film vyšiel

Tabuľka `age_group` obsahuje atribúty:
- id, typu INT, ktorý je primárny kľúč
- name, typu VARCHAR, ktorý udržiava názov danej vekovej skupiny (Pod 18 rokov, 18-24 rokov, ...)

Tabuľka `occupations` obsahuje atribúty:
- id, typu INT, ktorý je primárny kľúč
- name, typu VARCHAR, ktorý udržiava názov daného povolania

Tabuľka `genres_movies` je spojovacia tabuľka pre tabuľku `genres` a `movies` obsahuje atribúty:
- id, typu INT, ktorý je primárny kľúč
- movie_id, typu INT, ktorý je cudzím kľúčom tabuľy movies
- genre_id, typu INT, ktorý je cudzím kľúčom tabuľky genres

Tabuľka `genres` obsahuje atribúty:
- id, typu INT, ktorý je primárny kľúč
- name, typu VARCHAR, ktorý udržiava názov daného žánru

### **ERD diagram**
Surové dáta sú usporiadané v relačnom modeli, ktorý je znázornený na **entitno-relačnom diagrame (ERD)**:

<p align="center">
  <img src="https://github.com/filippresnajder/movielens/blob/main/erd_schema.png" alt="ERD Schema">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma MovieLens</em>
</p>

---
## **2 Dimenzionálny model**

Navrhnutý bol **hviezdicový model (star schema)**, pre efektívnu analýzu kde centrálny bod predstavuje faktová tabuľka **`fact_ratings`**, ktorá je prepojená s nasledujúcimi dimenziami:
- **`dim_users`**: Obsahuje informácie o uživateľoch (pohlavie, veková skupina, povolanie).
- **`dim_movies`**: Obsahuje podrobnejšie informácie o filmoch (názov, rok vydania, žánre, tagy).
- **`dim_date`**: Zahrňuje informácie o dátumoch hodnotení (dátum, deň, mesiac, rok, týždeň, deň v týždni, deň v týždni ako reťazec, mesiac ako reťazec).
- **`dim_time`**: Obsahuje podrobné časové údaje (čas, hodina).

Štruktúra hviezdicového modelu je znázornená na diagrame nižšie. Diagram ukazuje prepojenia medzi faktovou tabuľkou a dimenziami, čo zjednodušuje pochopenie a implementáciu modelu.

<p align="center">
  <br>
  <img src="https://github.com/filippresnajder/movielens/blob/main/star_schema.png" alt="Star Schema">
  <br>
  <em>Obrázok 2 Schéma hviezdy pre MovieLens</em>
</p>

---
## **3. ETL proces v Snowflake**
ETL proces pozostával z troch hlavných fáz: `extrahovanie` (Extract), `transformácia` (Transform) a `načítanie` (Load). Tento proces bol implementovaný v Snowflake s cieľom pripraviť zdrojové dáta zo staging vrstvy do viacdimenzionálneho modelu vhodného na analýzu a vizualizáciu.

---
### **3.1 Extract (Extrahovanie dát)**
Dáta zo zdrojového datasetu (formát `.csv`) boli najprv nahraté do Snowflake prostredníctvom interného stage úložiska s názvom `my_stage`. Stage v Snowflake slúži ako dočasné úložisko na import alebo export dát. Vytvorenie stage bolo zabezpečené príkazom:

#### Príklad kódu:
```sql
CREATE OR REPLACE STAGE my_stage;
```
Do stage boli následne nahraté súbory obsahujúce údaje o filmoch, používateľoch, hodnoteniach a zamestnaniach. Dáta boli importované do staging tabuliek pomocou príkazu `COPY INTO`. Pre každú tabuľku sa použil podobný príkaz:

```sql
COPY INTO occupations_staging
FROM @my_stage/occupations.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);
```

V prípade nekonzistentných záznamov bol použitý parameter `ON_ERROR = 'CONTINUE'`, ktorý zabezpečil pokračovanie procesu bez prerušenia pri chybách.

---
### **3.2 Transform (Transformácia dát)**

V tejto fáze boli dáta zo staging tabuliek vyčistené, transformované a obohatené. Hlavným cieľom bolo pripraviť dimenzie a faktovú tabuľku, ktoré umožnia jednoduchú a efektívnu analýzu.

Dimenzie boli navrhnuté na poskytovanie kontextu pre faktovú tabuľku. `Dim_users` obsahuje údaje o používateľoch vrátane vekových kategórií, pohlavia, zamestnania. Transformácia zahŕňala rozdelenie veku používateľov do kategórií (napr. „18-24“) a pridanie popisov zamestnaní. Táto dimenzia je typu SCD 2, čo umožňuje sledovať historické zmeny v zamestnaní.
```sql
CREATE TABLE dim_users AS
SELECT DISTINCT
    u.user_id AS dim_userId,
    u.gender AS gender,
    a.name AS age_group,
    o.name AS occupation
FROM users_staging u
JOIN age_group_staging a ON u.age = a.age_group_id
JOIN occupations_staging o ON u.occupation_id = o.occupation_id;
```

Dimenzia `dim_movies` obsahuje údaje o filmoch, ako sú názov, rok vydania, žánre a tagy. Táto dimenzia je typu SCD Typ 1, nakoľko údaje môžu byť aktualizované, napríklad tak, že užívateľ pridá nový tag filmu. Dôležité je povšimnúť si `LEFT JOIN` pri tags_staging. Toto je z dôvodu, že film, má napríklad vždy nejaký žáner, ale môže sa stať prípad, kde žiaden uživateľ nezadal filmu žiaden tag, takže musíme ošetriť aj záznamy, ktoré su `NULL`.
```sql
CREATE TABLE dim_movies AS
SELECT DISTINCT
    m.movie_id AS movie_id,
    m.title AS title,
    m.release_year AS release_year,
    g.name AS genres,
    t.tags AS tag_name
FROM movies_staging m
JOIN genres_movies_staging gm ON gm.movie_id = m.movie_id
JOIN genres_staging g ON g.genres_id = gm.genre_id
LEFT JOIN tags_staging t ON t.movie_id = m.movie_id;
```

Dimenzia `dim_date` je navrhnutá tak, aby uchovávala informácie o dátumoch hodnotení filmov. Obsahuje odvodené údaje, ako sú deň, mesiac (v textom aj číselnom formáte), rok, deň v týždni (v textovom aj číselnom formáte). Táto dimenzia je štruktúrovaná tak, aby umožňovala podrobné časové analýzy, ako sú trendy hodnotení podľa dní, mesiacov alebo rokov. Z hľadiska SCD je táto dimenzia klasifikovaná ako SCD Typ 0. To znamená, že existujúce záznamy v tejto dimenzii sú nemenné a uchovávajú statické informácie.

V prípade, že by bolo potrebné sledovať zmeny súvisiace s odvodenými atribútmi (napr. pracovné dni vs. sviatky), bolo by možné prehodnotiť klasifikáciu na SCD Typ 1 (aktualizácia hodnôt) alebo SCD Typ 2 (uchovávanie histórie zmien). V aktuálnom modeli však táto potreba neexistuje, preto je `dim_date` navrhnutá ako SCD Typ 0 s rozširovaním o nové záznamy podľa potreby.

```sql
CREATE TABLE dim_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY rated_at) AS dim_dateID,
    CAST(rated_at AS DATE) AS date,  
    DATE_PART(day, rated_at) AS day,
    DATE_PART(month, rated_at) AS month,
    DATE_PART(year, rated_at) AS year,                
    DATE_PART(week, rated_at) AS week,
    DATE_PART(dow, rated_at) + 1 AS day_of_week,        
    CASE DATE_PART(dow, rated_at) + 1
        WHEN 1 THEN 'Pondelok'
        WHEN 2 THEN 'Utorok'
        WHEN 3 THEN 'Streda'
        WHEN 4 THEN 'Štvrtok'
        WHEN 5 THEN 'Piatok'
        WHEN 6 THEN 'Sobota'
        WHEN 7 THEN 'Nedeľa'
    END AS day_of_week_string,             
    CASE DATE_PART(month, rated_at)
        WHEN 1 THEN 'Január'
        WHEN 2 THEN 'Február'
        WHEN 3 THEN 'Marec'
        WHEN 4 THEN 'Apríl'
        WHEN 5 THEN 'Máj'
        WHEN 6 THEN 'Jún'
        WHEN 7 THEN 'Júl'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'Október'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END AS month_string         
FROM ratings_staging
GROUP BY rated_at,
         DATE_PART(day, rated_at),
         DATE_PART(month, rated_at), 
         DATE_PART(year, rated_at), 
         DATE_PART(week, rated_at), 
         DATE_PART(dow, rated_at);
```

Dimenzia `dim_time` je navrhnutá tak, aby udržiavala časový údaj hodnotenia a samotnú hodinu hodnotenia, Z hľadiska SCD je táto dimenzia klasifikovaná ako SCD Typ 0. To znamená, že existujúce záznamy v tejto dimenzii sú nemenné a uchovávajú statické informácie.
```sql
CREATE TABLE dim_time AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY DATE_TRUNC('HOUR', r.rated_at)) AS dim_timeID,
    TO_TIMESTAMP(r.rated_at) AS time,
    TO_NUMBER(TO_CHAR(r.rated_at, 'HH24')) AS hour
FROM ratings_staging r
GROUP BY r.rated_at;
```

Faktová tabuľka `fact_ratings` obsahuje záznamy o hodnoteniach a prepojenia na všetky dimenzie. Obsahuje kľúčové metriky, ako je hodnota hodnotenia a časový údaj.
```sql
CREATE TABLE fact_ratings AS
SELECT DISTINCT
       r.rating_id AS ratingId,
       r.rated_at AS rating_datetime,
       r.rating AS rating,
       du.dim_userid AS user_id,
       dm.movie_id AS movie_id,
       dd.dim_dateid AS date_id,
       dt.dim_timeid AS time_id
FROM ratings_staging r
JOIN dim_date dd ON CAST(r.rated_at AS DATE) = dd.date
JOIN dim_time dt ON TO_TIMESTAMP(r.rated_at) = dt.time
JOIN dim_users du ON du.dim_userid = r.user_id
JOIN dim_movies dm ON dm.movie_id = r.movie_id;
```

---
### **3.3 Load (Načítanie dát)**

Po úspešnom vytvorení dimenzií a faktovej tabuľky boli dáta nahraté do finálnej štruktúry. Na záver boli staging tabuľky odstránené, aby sa optimalizovalo využitie úložiska:
```sql
DROP TABLE occupations_staging;
DROP TABLE age_group_staging;
DROP TABLE genres_staging;
DROP TABLE users_staging;
DROP TABLE movies_staging;
DROP TABLE tags_staging;
DROP TABLE genres_movies_staging;
DROP TABLE ratings_staging;
```
ETL proces v Snowflake umožnil spracovanie pôvodných dát z `.csv` formátu do viacdimenzionálneho modelu typu hviezda. Tento proces zahŕňal čistenie, obohacovanie a reorganizáciu údajov. Výsledný model umožňuje analýzu čitateľských preferencií a správania používateľov, pričom poskytuje základ pre vizualizácie a reporty.

---

## **4 Vizualizácia dát**

Dashboard obsahuje `5 vizualizácií`, ktoré poskytujú základný prehľad o kľúčových metrikách a trendoch týkajúcich sa filmov, používateľov a hodnotení. Tieto vizualizácie odpovedajú na dôležité otázky a umožňujú lepšie pochopiť správanie používateľov a ich preferencie.

<p align="center">
  <img src="https://github.com/filippresnajder/movielens/blob/main/movielens_dashboard.png" alt="Dashboard">
  <br>
  <em>Obrázok 3 Dashboard Movielens datasetu</em>
</p>

---
### **Graf 1: Počet hodnotení pre jednotlivé pohlavie**
Graf znázorňuje rozdiely v počte hodnotení medzi mužmi a ženami. Z údajov je zrejmé, že muži hodnotili filmy výrazne viac. Táto vizualizácia ukazuje, že obsah alebo kampane by museli byť zamerané skôr na jedno pohlavie, prípadne by bolo možné aj zamerať kampaň na získanie viac používateľov ženského pohlavia.

```sql
SELECT d.gender AS Pohlavie, COUNT(f.ratingid) AS Pocet
FROM fact_ratings f
JOIN dim_users d ON d.dim_userid = f.user_id
GROUP BY Pohlavie;
```
---
### **Graf 2: Najlepšie hodnotené filmy so 100 alebo viac hodnoteniami (Top 10)**
Graf znázorňuje 10 najlepšie hodnotených filmov (ktoré majú aspoň 100 alebo viac hodnotení, aby sa vyhlo filmom, ktoré by mali napríklad len 10 hodnotení, ktoré by nemuseli byť dostatočné na usúdenie kvality filmu). Vizualizácia ukazuje filmy a ich priemerné hodnotenie, na základe tejto vizualizácie dokážeme určiť, ktoré filmy sa páčili ľudom najviac a vieme ich prípadne odporúčiť novým uživateľom.

```sql
SELECT m.title AS Film, ROUND(AVG(f.rating), 2) Hodnotenie
FROM fact_ratings f
JOIN dim_movies m ON m.movie_id = f.movie_id
GROUP BY Film HAVING COUNT(f.movie_id) >= 100
ORDER BY Hodnotenie DESC LIMIT 10;
```
---
### **Graf 3: Počet hodnotení v jednotlivých hodinách dňa**
Graf ukazuje, v ktorých hodinách sú uživatelia najaktívnejší, z grafu sa dá jednoznačne vyčítať, že úžívatelia sú najaktívnejší vo večerných a skorých ranných hodinách s tendenciou klesania až po obed a následne od obeda počet hodnotení stúpa. Tieto údaje vieme využiť pre prípadne určenie časových intervalov, kedy by sme mali aktualizovať stránky o nový obsah.

```sql
SELECT t.hour AS Hodina, COUNT(f.ratingid) AS Pocet
FROM fact_ratings f
JOIN dim_time t ON t.dim_timeid = f.time_id
GROUP BY Hodina
ORDER BY POCET DESC;
```
---
### **Graf 4: Počet hodnotení v jednotlivých dňoch týždňa**
Tento graf poskytuje informácie o aktivite používateľov v jednotlivých dňoch týždňa. Z grafu vieme vyčítať, že používatelia sú najaktívnejší v Utorok a potom v Stredu. Na základe týchto dát a dát s časovými údajmi dokážeme predpovedať čas, kedy by mali byť stránky najaktívnejšie a k tomu prispôsobiť našu stratégiu (napr. aktualizácia kontentu).

```sql
SELECT d.day_of_week_string AS Den, COUNT(f.ratingid)
FROM fact_ratings f
JOIN dim_date d ON d.dim_dateid = f.date_id
GROUP BY Den;
```
---
### **Graf 5: Najlepšie hodnotené žánre (Top 10)**
Tento graf ukazuje 10 žánrov, ktoré sú najlepšie hodnotené medzi úživateľmi v priemernom hodnotení. Tieto údaje vieme využiť napríklad na stratégiu odporúčania filmov s týmito žánrami pre nových uživateľov.

```sql
SELECT m.genres AS Zaner, ROUND(AVG(f.rating), 2) Hodnotenie
FROM fact_ratings f
JOIN dim_movies m ON m.movie_id = f.movie_id
GROUP BY Zaner
ORDER BY Hodnotenie DESC LIMIT 10;
```

Dashboard poskytuje komplexný pohľad na dáta, pričom zodpovedá dôležité otázky týkajúce sa sledovateľských preferencií a správania používateľov. Vizualizácie umožňujú jednoduchú interpretáciu dát a môžu byť využité na optimalizáciu odporúčacích systémov, marketingových stratégií a filmových služieb.

---

**Autor:** Filip Prešnajder
