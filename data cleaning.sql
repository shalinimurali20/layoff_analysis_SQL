-- Data Cleaning Project 

-- Created SQL Schema - layoffs
USE layoffs;

-- Imported .csv file as 'layoffs' table with 2361 records
SELECT *
FROM layoffs;

-- Part 1: Remove Duplicates
-- Creating a copy of layoffs table to perform tasks that does not affect raw file
CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging;

-- There exists no unique identifier in the table to help us check duplicates
-- Create row_num to identify rows with duplicates and remove them from the database
-- As there are no unique identifiers, we partition using all columns to find duplicates
-- If there exists duplicates, the row_num would be greater than 1

SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- To display duplicates, we create duplicates_cte to find row_num > 1

WITH duplicates_cte AS (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging)

-- We can do the same by having a subquery

SELECT *
FROM (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging) AS dup
WHERE row_num > 1;

-- Deleting duplicates
-- Go to Navigator, Right click on 'layoffs_staging', 
-- Select 'Copy to Clipboard'-> Create Statement

CREATE TABLE `layoffs_dup_removed` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- We want to create a copy of 'layoffs_staging' but with row_num
INSERT layoffs_dup_removed
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Deleting records with row_num > 1 - deletes DUPLICATES
DELETE 
FROM layoffs_dup_removed
WHERE row_num > 1;

-- Viewing Results
SELECT *
FROM layoffs_dup_removed;

-- Part 2: Standardisation
-- We will be looking at each column one by one and standardise accordingly
-- However, in this part, we will not deal with NULL values.
-- Note: Do not make changes to raw table - layoffs

-- Company 
SELECT DISTINCT company
FROM layoffs_dup_removed;

-- There exists whitespaces before and after the name of the comapny
UPDATE layoffs_dup_removed
SET company = TRIM(company);

-- Location
SELECT DISTINCT location
FROM layoffs_dup_removed
ORDER BY 1;

-- Industry
SELECT DISTINCT industry
FROM layoffs_dup_removed
ORDER BY 1;

-- Looks like Crypto has 3 different entries - Crypto, CryptoCurrency, Crypto Currency
UPDATE layoffs_dup_removed
SET industry = 'Crypto' 
WHERE industry LIKE 'Crypto%';

-- Total Laid Off
SELECT total_laid_off
FROM layoffs_dup_removed;

-- Looks fine except for the NULL values which will be dealt later

-- Percentage Laid Off
SELECT percentage_laid_off
FROM layoffs_dup_removed;

-- The datatype for this column is text. Modifying it to Decimal (5,4).
ALTER TABLE layoffs_dup_removed
MODIFY COLUMN percentage_laid_off DECIMAL(5,2);

-- Date
UPDATE layoffs_dup_removed
SET `date` = str_to_date(`date`,'%m/%d/%Y'); -- YYYY-MM-DD format

SELECT `date`
FROM layoffs_dup_removed;

-- The datatype for this column is text. Modifying it to date.
ALTER TABLE layoffs_dup_removed
MODIFY COLUMN `date` DATE;

-- Stage
SELECT DISTINCT stage
FROM layoffs_dup_removed
ORDER BY 1;

-- Looks fine except for the NULL value which will be dealt later

-- Country
SELECT DISTINCT country
FROM layoffs_dup_removed
ORDER BY 1;

-- There exists two United States with and without a '.'
SELECT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_dup_removed
WHERE country LIKE 'United States%';

UPDATE layoffs_dup_removed
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Funds raised millions
SELECT funds_raised_millions
FROM layoffs_dup_removed;

-- Looks fine except for the NULL values which will be dealt later

-- Part 3: Null Values and Removal of Records/Columns
SELECT *
FROM layoffs_dup_removed
WHERE industry IS NULL;

-- Bally's Interactive has no industry and laid_off details; it is also not possible to populate
-- data as there exists no other information; however we may employ webscraping to fetch data, but, it is beyond the scope of this project
-- Given the analysis would be about layoffs, it is okay to remove this record.

DELETE 
FROM layoffs_dup_removed
WHERE company LIKE 'Bally%';

SELECT *
FROM layoffs_dup_removed
WHERE industry IS NULL OR industry = '';

-- There exists three records with industry being empty. Let's check if we can repopulate the data
SELECT *
FROM layoffs_dup_removed
WHERE company IN ('Airbnb','Carvana','Juul');

-- As atleast one of the rows contain 'industry', we can use it to repopulate data

-- Self Join

UPDATE layoffs_dup_removed t1
JOIN layoffs_dup_removed t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry = '' OR TRIM(t1.industry) = '') AND t2.industry IS NOT NULL;

-- Doesn't work not sure why
-- ChatGPT response: Your SQL syntax seems correct for standard SQL usage in MySQL. However, ensure that your database server is not operating with specific configurations that might affect join updates.

-- Changed empty values to NULL
UPDATE layoffs_dup_removed
SET industry = NULL 
WHERE industry = '';

-- Update NULL values to corresponding industry
UPDATE layoffs_dup_removed t1
JOIN layoffs_dup_removed t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_dup_removed
WHERE industry IS NULL;

-- Given the analysis is requires laid off values and percentage laid off, it doesn't make sense 
-- to have records with empty or NULL values in these columns

SELECT *
FROM layoffs_dup_removed
WHERE (total_laid_off IS NULL OR total_laid_off ='')  AND 
(percentage_laid_off IS NULL);


DELETE 
FROM layoffs_dup_removed
WHERE (total_laid_off IS NULL OR total_laid_off ='')  AND 
(percentage_laid_off IS NULL);

-- As we do not have a total or a value through which we can derive either laid off or 
-- percentage %, we leave it as it is.

SELECT *
FROM layoffs_dup_removed
WHERE `date` IS NULL;

-- Blackbaud does not have date entry. But it can still be considered for the analysis.

SELECT *
FROM layoffs_dup_removed
WHERE stage IS NULL;

-- Checking if we have any other data to help us repopulate
SELECT *
FROM layoffs_dup_removed
WHERE company IN (SELECT company
FROM layoffs_dup_removed
WHERE stage IS NULL);

-- But it can still be considered for the analysis.

SELECT *
FROM layoffs_dup_removed
WHERE country IS NULL OR country = '';

-- Perfect.

-- Dropping row_num

ALTER TABLE layoffs_dup_removed
DROP COLUMN row_num;

SELECT COUNT(company)
FROM layoffs_dup_removed;

SELECT *
FROM layoffs_dup_removed;
-- Data Cleaning is now succcessfully done. 
