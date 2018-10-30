# Table of Contents
1. [Problem](README.md#problem)
2. [Approach](README.md#approach)
3. [Run Instructions](README.md#run-instructions)
2. [Input Dataset](README.md#input-dataset)
3. [Instructions](README.md#instructions)
4. [Output](README.md#output)
5. [Tips on getting an interview](README.md#tips-on-getting-an-interview)
6. [Instructions to submit your solution](README.md#instructions-to-submit-your-solution)
7. [FAQ](README.md#faq)
8. [Questions?](README.md#questions?)

# Problem

A newspaper editor was researching immigration data trends on H1B(H-1B, H-1B1, E-3) visa application processing over the past years, trying to identify the occupations and states with the most number of approved H1B visas. She has found statistics available from the US Department of Labor and its [Office of Foreign Labor Certification Performance Data](https://www.foreignlaborcert.doleta.gov/performancedata.cfm#dis). But while there are ready-made reports for [2018](https://www.foreignlaborcert.doleta.gov/pdf/PerformanceData/2018/H-1B_Selected_Statistics_FY2018_Q4.pdf) and [2017](https://www.foreignlaborcert.doleta.gov/pdf/PerformanceData/2017/H-1B_Selected_Statistics_FY2017.pdf), the site doesn’t have them for past years. 

As a data engineer, you are asked to create a mechanism to analyze past years data, specificially calculate two metrics: **Top 10 Occupations** and **Top 10 States** for **certified** visa applications.

Your code should be modular and reusable for future. If the newspaper gets data for the year 2019 (with the assumption that the necessary data to calculate the metrics are available) and puts it in the `input` directory, running the `run.sh` script should produce the results in the `output` folder without needing to change the code.

# Approach

As a data engineer for a newspaper, I thought it would be helpful to build a reusable tool to count entries in in specified columns under constraints by other columns. Python was chosen as the development language to allow for ease of use by researchers and integration into common ETL tools, as well as rapid development and extensibility by users needing additional features. It uses an aliasing system to allow users to process multiple files with inconvenient or variable naming conventions. It is meant to be generalizable and reusable for future cases. It is primarily composed of two iterators, `CleanReader`, which iterates over rows of a csv file, and `MultiFileCounter`, which iterates over multiple files.

**Iterators**
`CleanReader`: Takes the path to a csv and the desired output columns and preprocesses the text in those fields, yielding a namedtuple. Optionally accepts an alias dict to use aliases for the namedtuple. The preprocessing steps include encoding and decoding to ensure integrity. The preprocess

`MultiFileCounter`: Takes a list of file paths to parse and, optionally, a dictionary which specifies aliases and corresponding column names. The user can dynamically add counters, constraints, and aliases. A `CleanReader` is instantiated for each file, with the necessary column names to apply the constraints and counters.

**H1B Statistics**
A `MultiFileCounter` is instantiated with aliases for 'status', 'occupation', and 'state'. Counters are added for 'occupation' and 'state', and a constraint is added for 'status' being 'certified'. Then the `write_output_file` function is run on each of the 'occupation' and 'state' counters, which uses `gen_counter_text` to generate text with the names, counts, and percentage for each of the counters. The `write_output_file` function takes the parameters: `filepath', 'header`, `counter`, `n`, and `delimmiter`. The `header` is an iterable of the desired column names, `n` is the number of top entries to include.

# Run Instructions

**H1B Statistics**
1. Download the repository  
`$git clone https://github.com/TheAustinator/h1b_statistics.git`  
2. Run the `run.sh` bash script  
`bash .h1b_statistics/run.sh`  

**General Use**
1. Download the repository  
```
$git clone https://github.com/TheAustinator/h1b_statistics.git
```
2. Import the `MultiFileCounter`  
3. Instantiate the `MultiFileCounter` with desired filepaths and aliases  
```
files = ['dir/file_1.csv', 'dir/file_2.csv', 'dir/file_3.csv']</
colname_dict = {  
      'population': ['POP_METRO', 'POPULATION', 'POP_TOT'],  
      'region': ['REGION', 'RGN'],  
}  
mfc = MultiFileCounter(files, colname_dict)
```
4. Add counters and constraints, and dynamically add additional aliases as needed  
```
mfc.add_constraint('population', '>', 50000)  
mfc.add_counter('region')  
mfc.add_alias('religion', 'MAJ_RELIGION')  
mfc.add_alias('religion', 'RELIGION')  
mfc.add_counter('religion')
```
5. Run counter  
```
mfc.count()
```
6. Extract counters  
```
counter_region = mfc.counters['region']  
counter_religion = mfc.counters['religion']
```
7. Use counters as desired, or use the `write_output_file` function to generate a text file of the top entries with the counts and percentages of total for each counter  
```
header = ('religion', 'disciples', 'percentage')  
write_output_file('top_5_religions.txt', header, counter_religion, 5, ',')
```
