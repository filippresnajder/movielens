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

### **ERD diagram**
Surové dáta sú usporiadané v relačnom modeli, ktorý je znázornený na **entitno-relačnom diagrame (ERD)**:

<p align="center">
  <img src="https://github.com/filippresnajder/movielens/blob/main/erd_schema.png" alt="ERD Schema">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma AmazonBooks</em>
</p>

---
## **2 Dimenzionálny model**

Navrhnutý bol **hviezdicový model (star schema)**, pre efektívnu analýzu kde centrálny bod predstavuje faktová tabuľka **`fact_ratings`**, ktorá je prepojená s nasledujúcimi dimenziami:
- **`dim_users`**: Obsahuje informácie o uživateľoch (pohlavie, veková skupina, povolanie).
- **`dim_movies`**: Obsahuje podrobnejšie informácie o filmoch (názov, rok vydania, žánre).
- **`dim_date`**: Zahrňuje informácie o dátumoch hodnotení (deň, mesiac, rok).
- **`dim_time`**: Obsahuje podrobné časové údaje (hodina, AM/PM).

Štruktúra hviezdicového modelu je znázornená na diagrame nižšie. Diagram ukazuje prepojenia medzi faktovou tabuľkou a dimenziami, čo zjednodušuje pochopenie a implementáciu modelu.

<p align="center">
  <img src="https://github.com/filippresnajder/movielens/blob/main/star_schema.png" alt="Star Schema">
  <br>
  <em>Obrázok 2 Schéma hviezdy pre AmazonBooks</em>
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
Do stage boli následne nahraté súbory obsahujúce údaje o knihách, používateľoch, hodnoteniach, zamestnaniach a úrovniach vzdelania. Dáta boli importované do staging tabuliek pomocou príkazu `COPY INTO`. Pre každú tabuľku sa použil podobný príkaz:

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
Podobne `dim_movies` obsahuje údaje o filmoch, ako sú názov, rok vydania a žánre. Táto dimenzia je typu SCD Typ 0, pretože údaje o filmoch sú považované za nemenné, napríklad názov filmu sa nemení. 

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

Dashboard obsahuje `6 vizualizácií`, ktoré poskytujú základný prehľad o kľúčových metrikách a trendoch týkajúcich sa filmov, používateľov a hodnotení. Tieto vizualizácie odpovedajú na dôležité otázky a umožňujú lepšie pochopiť správanie používateľov a ich preferencie.

<p align="center">
  <img src="https://github.com/filippresnajder/movielens/blob/main/movielens_dashboard.png" alt="Dashboard">
  <br>
  <em>Obrázok 3 Dashboard Movielens datasetu</em>
</p>

---
### **Graf 1: 10 Najviac hodnotených filmov**
Táto vizualizácia zobrazuje 10 filmov s najväčším počtom hodnotení. Umožňuje identifikovať najpopulárnejšie filmy medzi používateľmi. Z grafu vieme zistiť napríklad, že filmy Star Wars sú najviac hodnotené, na základe tohoto môžeme spraviť odporúčania pre napr. jednotlivé série filmov.

```sql
SELECT m.title AS Movie_Name, COUNT(ratingid) AS Total_Ratings
FROM fact_ratings f
JOIN dim_movies m ON m.movie_id = f.movie_id
GROUP BY Movie_Name
ORDER BY Total_Ratings DESC
LIMIT 10;
```
---
### **Graf 2: Podiel hodnotení podľa pohlaví**
Graf znázorňuje rozdiely v počte hodnotení medzi mužmi a ženami. Z údajov je zrejmé, že muži hodnotili filmy výrazne viac. Táto vizualizácia ukazuje, že obsah alebo kampane by museli byť zamerané skôr na jedno pohlavie, prípadne by bolo možné aj zamerať kampaň na získanie viac používateľov ženského pohlavia.

```sql
SELECT u.gender, COUNT(r.ratingid) AS Pocet
FROM fact_ratings r
JOIN dim_users u ON u.dim_userid = r.user_id
GROUP BY u.gender;
```
---
### **Graf 3: 15 Najlepšie hodnotených filmov podľa priemerného hodnotenia**
Graf ukazuje, ktoré filmy boli najlepšie hodnotené na základe priemerného hodnotenia, z vizualizácie je vidieť, že filmov s perfektným hodnotením je málo. Tento trend môže naznačovať kvalitu filmov, ktoré môžu byť odporúčané používateľom na ich zhliadnutie.

```sql
SELECT m.title AS Film, ROUND(AVG(r.rating),2) AS Priemerne_Hodnotenie
FROM fact_ratings r
JOIN dim_movies m ON m.movie_id = r.movie_id
GROUP BY Film
ORDER BY Priemerne_Hodnotenie DESC
LIMIT 15;
```
---
### **Graf 4: Povolania, ktoré hodnotili filmy najviac**
Tento graf poskytuje informácie o piatich povolaní, ktoré hodnotia filmy najviac. Umožňuje zistovať, ktoré profesie najviac hodnototia a sledujú filmy a ako môžu byť tieto skupiny zacielené pri vytváraní personalizovaných odporúčaní. Z údajov je zrejmé, že najaktívnejšími profesijnými skupinami sú `Educator` a `Executive`, ak odmyslíme ľudí, ktorí buď nemajú alebo majú nezahrnuté povolanie. 

```sql
SELECT u.occupation AS Povolanie, COUNT(r.ratingid) AS Pocet
FROM fact_ratings r
JOIN dim_users u ON u.dim_userid = r.user_id
GROUP BY Povolanie
ORDER BY Pocet DESC
LIMIT 5;
```
---
### **Graf 5: Najaktívnejšie mesiace na základe hodnotenia**
Tento graf ukazuje, v ktorých mesiacov sú uživateľia najviac aktívny. Z grafu vieme usúdiť,
že v mesiaci November sú používateľia suverénne najviac aktívny. Na základe týchto údajov vieme
nastaviť v akom období, budeme propagovať filmy najviac alebo kedy sa nám oplatí aplikovať
naše marketingové stratégie v praxi.

```sql
SELECT d.month_string AS Mesiac, COUNT(r.ratingid) AS Pocet
FROM fact_ratings r
JOIN dim_date d ON d.dim_dateid = r.date_id
GROUP BY Mesiac
ORDER BY Pocet DESC;
```
---
### **Graf 6: Rozdelenie hodnotení podľa vekových kategórii v daných časových údajoch**
Tento stĺpcový graf ukazuje, ako sa aktivita používateľov mení počas dňa (dopoludnia vs. popoludnia) a ako sa líši medzi rôznymi vekovými skupinami. Z grafu vyplýva, že vekové kategórie v rozmedzí 25 až 34 rokov sú s prehľadom najviac aktívne a následne, čím je veková kategória staršia, tým aj klesá aj počet hodnotení. Tieto informácie môžu pomôcť lepšie zacieliť obsah a naplánovať ho pre jednotlivé vekové kategórie.
```sql
SELECT t.ampm AS Casovy_Udaj,
       u.age_group AS Vekova_Kategoria,
       COUNT(r.ratingid) AS Pocet
FROM fact_ratings r
JOIN dim_users u ON u.dim_userid = r.user_id
JOIN dim_time t ON t.dim_timeid = r.time_id
GROUP BY casovy_udaj, vekova_kategoria
ORDER BY casovy_udaj, Pocet DESC;

```

Dashboard poskytuje komplexný pohľad na dáta, pričom zodpovedá dôležité otázky týkajúce sa sledovateľských preferencií a správania používateľov. Vizualizácie umožňujú jednoduchú interpretáciu dát a môžu byť využité na optimalizáciu odporúčacích systémov, marketingových stratégií a filmových služieb.

---

**Autor:** Filip Prešnajder
