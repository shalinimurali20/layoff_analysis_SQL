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
