/**
  * This object will read bson file and create parquet file at target location.
  * File has been cleared for public display
  * 
  * @version 1.2
  * @author Charlie Rosado 08/06/2018
  * Licensed under Creative Commons CC BY, CC BY-SA, CC0
  */

/* Hulu data processing -
* creates a new column called 'service' that extracts the source of the feed from filename.
* The only method to determine the source of the data (HBO vs Cinemax) is to extract the source from filename.
* The process herein scans the filename, identifies whether HBO or Cinemax and then places the value in a new column.
* Same rule is going to apply for partitioning.
* This file impacts the HuluData set which represents Hulu_User_Daily
*/


