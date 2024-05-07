-- LAYOFFS DATA CLEANING PROJECT
-- NOTICE: ALL steps are shown as a reference guide. These queries can be consolidated, but for reference purposes, they're labled to show my complete process. 
-- 1. PREVIEW DATA
-- 2. CREATE NEW TABLE TO CLEAN IN 
-- 3. REMOVE DUPLICATES
-- 4. STANDARDIZE DATA
-- 5. FIX NULLS
-- 6. CONSOLIDATE DATA FOR EXPLORATION

-- 1. PREVIEW DATA
SELECT * FROM world_layoffs.layoffs;

-- 2. CREATE NEW TABLE TO CLEAN IN 
CREATE TABLE world_layoffs.layoffs_update AS SELECT * FROM world_layoffs.layoffs;
SELECT * FROM world_layoffs.layoffs_update;

-- 3. REMOVE DUPLICATES
-- 3a. find the duplicates
SELECT *, 
ROW_NUMBER () OVER (
PARTITION BY company, industry, total_laid_off, `date`) AS row_num 
FROM world_layoffs.layoffs_update;

WITH duplicate_cte AS 
(
SELECT *, 
ROW_NUMBER () OVER (
PARTITION BY company, industry, total_laid_off, `date`) AS row_num 
FROM world_layoffs.layoffs_update
)
SELECT *
FROM duplicate_cte
WHERE	row_num > 1;

-- 3b. check if these are actually duplicates 
SELECT *
FROM world_layoffs.layoffs_update
WHERE company = 'Oda';

-- 3c. they are not, so going back to partion every column 
WITH duplicate_cte AS 
(
SELECT *, 
ROW_NUMBER () OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date` , stage, country, funds_raised_millions) AS row_num 
FROM world_layoffs.layoffs_update
)
SELECT *
FROM duplicate_cte
WHERE	row_num > 1;

-- 3d. checking it they are true duplicates again
SELECT * 
FROM world_layoffs.layoffs_update
WHERE company = 'Casper';

-- 3e. They are, remove only the duplicates like shown for Casper. Do this by creating a new table like the CTE, to limit errors later on.
CREATE TABLE world_layoffs.layoffs_update2 (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * 
FROM world_layoffs.layoffs_update2;

INSERT INTO world_layoffs.layoffs_update2
SELECT *, 
ROW_NUMBER () OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date` , stage, country, funds_raised_millions) AS row_num 
FROM world_layoffs.layoffs_update;

-- confirm the changes are correct
SELECT * 
FROM world_layoffs.layoffs_update2
WHERE row_num > 1;

DELETE 
FROM world_layoffs.layoffs_update2
WHERE row_num > 1;

SELECT * 
FROM world_layoffs.layoffs_update2
WHERE row_num > 1;

-- 4. STANDARDIZE DATA

-- 4a. Look at each column for review
-- 4b. starting with company, spaces need to be trimmed 

SELECT company, TRIM(company)
FROM world_layoffs.layoffs_update2;

UPDATE world_layoffs.layoffs_update2
SET company = TRIM(company);

-- 4c. check industry column. Consolidate Crypto, Crypto Currency, and CryptoCurrency

SELECT DISTINCT industry
FROM world_layoffs.layoffs_update2
ORDER BY 1;

SELECT *
FROM world_layoffs.layoffs_update2
WHERE industry LIKE 'Crypto%';

UPDATE world_layoffs.layoffs_update2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- keep checking work
SELECT DISTINCT industry
FROM world_layoffs.layoffs_update2
ORDER BY 1;
-- 4d. check location

SELECT DISTINCT location
FROM world_layoffs.layoffs_update2
ORDER BY 1;

-- 4e. check country. consolidate the United States and United States. column

SELECT DISTINCT country
FROM world_layoffs.layoffs_update2
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM world_layoffs.layoffs_update2
ORDER BY 1;

UPDATE world_layoffs.layoffs_update2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- 4f. change date from txt to date
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y') as formatted_date
FROM world_layoffs.layoffs_update2;

UPDATE world_layoffs.layoffs_update2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
FROM world_layoffs.layoffs_update2;

-- 4g. change the schema for date from text to date

ALTER TABLE world_layoffs.layoffs_update2
MODIFY COLUMN `date` DATE;

-- 5. FIX NULLS
-- 5a. preview nulls for total_laod off and percentage_laid_off which is where i noticed most nulls earlier on
SELECT *
FROM world_layoffs.layoffs_update2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- 5b1. preview nulls for industry, which i also noticed earlier on
SELECT *
FROM world_layoffs.layoffs_update2
WHERE industry IS NULL
OR industry = '';

UPDATE world_layoffs.layoffs_update2
SET industry = NULL
WHERE industry = '';

-- 5b2. populate industry columns
SELECT *
FROM world_layoffs.layoffs_update2
WHERE company = 'Airbnb';

SELECT * 
FROM world_layoffs.layoffs_update2 t1
JOIN world_layoffs.layoffs_update2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL)
AND t2.industry IS NOT NULL;

UPDATE world_layoffs.layoffs_update2 t1
JOIN world_layoffs.layoffs_update2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL)
AND t2.industry IS NOT NULL;

-- check for nulls in industry
SELECT *
FROM world_layoffs.layoffs_update2
WHERE industry IS NULL;

SELECT *
FROM world_layoffs.layoffs_update2
WHERE company LIKE 'Bally%';

/* there are no other row's with Bally's Interactive, to take this further, data can be scraped from the web to fix this null. 
that also applies to nulls in total_laid_off and percetage_laid_off columns. that is a different project. 
therefore, the cleaning process proceeds with what is available .*/

-- 5c. create a new table that deletes rows in the updated table that have nulls for total_laid_off and percentage_laid_off
CREATE TABLE world_layoffs.layoffs_update3 AS SELECT * FROM world_layoffs.layoffs_update2;
SELECT * FROM world_layoffs.layoffs_update3;

DELETE 
FROM world_layoffs.layoffs_update3
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL; 

-- 6. CONSOLIDATE DATA FOR EXPLORATION
-- 6a. remove the row_num column from earlier
ALTER TABLE world_layoffs.layoffs_update3
DROP COLUMN row_num;

SELECT * 
FROM world_layoffs.layoffs_update3;




