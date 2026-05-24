# Házi Feladat Specifikáció
**Data Engineering** - Opcionális házi feladat

---

## 1. Hallgató adatai

| | |
|---|---|
| **Név** | Ács Dorottya |
| **Neptun-kód** | BYXM73 |
| **E-mail** | acsdoro2005@gmail.com |

---

## 2. Témaválasztás

| | |
|---|---|
| **Választott téma** | Steam játék statisztikák és trendek elemzése |

**Rövid leírás** *(2–4 mondat: milyen üzleti/elemzési kérdést old meg a pipeline? Milyen forrásadatokból indul ki, és milyen eredményt produkál?)*

> A pipeline célja a Steam webáruház játékainak népszerűségi és ár alapú elemzése. A rendszer egy nyilvános datasetből és a Steam Web API-ból származó aktuális játékosszám adatokból dolgozik. Az adatok tisztítása és egyesítése után különböző aggregációk és statisztikák készülnek, például a legnépszerűbb játékok, valamint az ár és játékosszám közötti kapcsolat vizsgálata. Az eredmény egy lekérdezhető adattárház és egyszerű vizualizációk.

---

## 3. Tervezett pipeline elemei

| Elem | Tervezett megoldás / eszköz |
|---|---|
| **Adatforrások** *(min. 2)* | Steam dataset (Kaggle), Steam Web API |
| **Feldolgozási mód** | Batch |
| **Landing zone** *(nyers tároló)* | lokális fájlrendszer |
| **Adatmodell típusa** | Csillag séma – 2 ténytábla + 5 dimenziótábla + 1 hídtábla  |
| **Adattárház / adatplatform** | PostgreSQL |
| **Transzformáció** | Pandas |
| **Orchestration eszköz** | Prefect |
| **Infrastruktúra** | Docker Compose |
| **Adatkiszolgálás** | SQL nézetek, Metabase dashboard |

---