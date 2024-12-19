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
