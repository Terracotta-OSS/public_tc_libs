This resource directory contains data files obtained from public sources.


cities.csv
    Obtained from Google AdWords API at https://developers.google.com/adwords/api/docs/appendix/geotargeting.
    The content of the file is described at that location.

    This dataset contains some universities but very limited; use of data from 'postscndryunivsrvy2013dirinfo.csv'
    is recommended.

NST-EST2015-01.csv
    Obtained from the United States Census Bureau "National Totals: Vintage 2015" at
    http://www.census.gov/popest/data/national/totals/2015/index.html.  The file is largely CSV but
    does carry a lot of "garbage" resulting in the need for non-standard parsing.  Note that the
    state names are preceded by a period.


dist.all.last
dist.female.first
dist.male.first
    Obtained from the United States Census Bureau "Frequently Occurring Surnames from Census 1990 – Names Files"
    http://www.census.gov/topics/population/genealogy/data/1990_census/1990_census_namefiles.html.  These files
    contain lines with whitespace-separated values where each line has:
        1) name
        2) frequency (percent)
        3) cumulative frequency (percent)
        4) rank
    The site has a document explaining the files in more detail.


postscndryunivsrvy2013dirinfo.csv
userssharedsdfpublicelementarysecondaryunivsrvy200910.csv
    Obtained from the US Department of Education through http://www.data.gov/education/developers
    "Education: Developers".  These files list the colleges and universities (postscndryunivsrvy...)
    and elementary and secondary schools (userssharedsdfpublicelementarysecondaryunivsrvy) in the
    US.  The schema of each file is listed in an associated document founds on the we site.

    The 'userssharedsdfpublicelementarysecondaryunivsrvy' dataset fields and encodings of interest are
    described here (extracted from the INsc09101a.pdf document accompanying the dataset):

        SCHNAM09 - Name of the school
        LCITY09  - School location city
        LSTATE09 - Two-letter U.S. Postal Service abbreviation of the state where the school address is located
        LZIP09   - Five-digit U.S. Postal Service ZIP code for the location address
        TYPE09   - NCES code for type of school
                1 = Regular school
                2 = Special education school
                3 = Vocational school
                4 = Other/alternative school
                5 = Reportable program (new code starting in 2007–08)
        STATUS09 - NCES code for the school status
                1 = School was operational at the time of the last report and is currently operational.
                2 = School has closed since the time of the last report.
                3 = School has been opened since the time of the last report.
                4 = School was operational at the time of the last report but was not on the CCD list at that time.
                5 = School was listed in previous year’s CCD school universe as being affiliated with a different
                education agency.
                6 = School is temporarily closed and may reopen within 3 years.
                7 = School is scheduled to be operational within 2 years.
                8 = School was closed on previous year’s file but has reopened.
        GSLO09   - School low grade offered
                UG = Ungraded
                PK = Prekindergarten
                KG = Kindergarten
                01–12 = 1st through 12th grade
                N = School had no students reported
        GSHI09   - School high grade offered
                UG = Ungraded
                PK = Prekindergarten
                KG = Kindergarten
                01–12 = 1st through 12th grade
                N = School had no students reported
        LEVEL09  - School level
                1 = Primary (low grade = PK through 03; high grade = PK through 08)
                2 = Middle (low grade = 04 through 07; high grade = 04 through 09)
                3 = High (low grade = 07 through 12; high grade = 12 only)
                4 = Other (any other configuration not falling within the above three categories, including ungraded)
        MEMBER09 - Total students, all grades: The reported total membership of the school

    The 'postsecndryunivsrvy' dataset fields and encodings of interest are described here (extracted from the
    HD2010.xls Frequencies spreadsheet accompanying the dataset):

        INSTNM	- Institution (entity) name
        CITY	- City location of institution
        STABBR	- State abbreviation
        ZIP     - ZIP code
        FIPS    - FIPS state code
        SECTOR  - One of nine institutional categories resulting from dividing the universe according to
                  control and level. Control categories are public, private not-for-profit, and private
                  for-profit. Level categories are 4-year and higher (4 year), 2-but-less-than 4-year
                  (2 year), and less than 2-year. For example: public, 4-year institutions.
                  Control - A classification of whether an institution is operated by publicly elected or
                      appointed officials (public control) or by privately elected or appointed officials
                      and derives its major source of funds from private sources (private control).
                  Level - A classification of whether an institution’s programs are 4-year or higher
                      (4 year), 2-but-less-than 4-year (2 year), or less than 2-year.
                  **** Used to weed-out non-schools. ****
                0	Administrative Unit
                1	Public, 4-year or above
                2	Private not-for-profit, 4-year or above
                3	Private for-profit, 4-year or above
                4	Public, 2-year
                5	Private not-for-profit, 2-year
                6	Private for-profit, 2-year
                7	Public, less-than 2-year
                8	Private not-for-profit, less-than 2-year
                9	Private for-profit, less-than 2-year
                99	Sector unknown (not active)
        HLOFFER - Highest level of offering
                1	Award of less than one academic year
                2	At least 1, but less than 2 academic yrs
                3	Associate's degree
                4	At least 2, but less than 4 academic yrs
                5	Bachelor's degree
                6	Postbaccalaureate certificate
                7	Master's degree
                8	Post-master's certificate
                9	Doctor's degree
                -3	{Not available}
        UGOFFER - A code indicating whether the institution offers undergraduate degrees or certificates.
                  Undergraduate degrees or certificates include associate's and bachelor's degrees, and
                  certificates that require less-than 4 academic years of study.
                1	Undergraduate degree or certificate offering
                2	No undergraduate offering
                -3	Not available
        INSTSIZE - Institution size category based on total students enrolled for credit
                1	Under 1,000
                2	1,000 - 4,999
                3	5,000 - 9,999
                4	10,000 - 19,999
                5	20,000 and above
                -1	Not reported
                -2	Not applicable