# TemporalDedup
Source code and datasets related to the TemporalDedup approach for deduplication of temporal data

Included in this repository is an Eclipse Java project that includes the source code, external libraries, PSN and COVID-19 datasets, and pre-configured launchers for the TemporalDedup application.  This represents an implementation of the concepts put forward in the Journal of Information Processing & Management article under consideration, "TemporalDedup: Domain-Independent Deduplication of Redundant and Errant Temporal Data."  The core concepts are implemented and adherent to the algorithms and descriptions put forward in the manuscript.  Future iterations of development will refactor the code for efficiency, add functionality/capability/robustness, and resolve bugs and limitations discovered through the application to additional datasets.

The datasets are zipped in the datasets subfolder.  To run the application, you will want to extract those zip files and copy the dataset and truth files to the input subfolder.  Included in each dataset zip file is README with more detail about the format and information of each dataset.

-------------------
Requirements/assumptions in the current revision (robustness updates planned for the future):

(a) Requires command line specification of a dataset file and a truth data file

(b) Assumes dataset file has a header row and is tab-delimited with no unique record IDs; will internally assign record IDs when parsing

(c) Assumes truth file has a header row, is tab-delimited, and the first value of each row is the record ID of a duplicate

(d) Assumes multiple timestamps (i.e. that the timestamp data is a repeating attribute with the same header name)

(e) For ASNM comparison method, user is to provide the blocking key attribute name as a command line argument when invoking the option to run the comparison method; if not is specified then the entire dataset will be treated as a  single block

(f) The default settings for model parameters is LCS max-sampling with an unconstrained order minimum sequence length of 8.

(g) Output files requested by the -a and/or -o flags on the command line will be written to ./output/ folder.

-------------------------------------------------------
COMMAND LINE ARGUMENTS (usage)

Required inputs:

-d "dataset filename" -t "truth data filename"
  
These inputs specify the filenames representing the dataset (-d) to be processed and the duplicate truth data (-f).

Optional inputs (related to ASNM comparison method):

-c "blocking key" -s "list of thresholds"

These inputs allow for the user specification of the attribute name to be used as the blocking key for ASNM and the list of similarity thresholds to be used to assert duplication by ASNM.  By default, there is no blocking key (entire dataset treated as one comparison block) and the list of thresholds are .8, .9, .927, .950, .963, .981, and 1.0.
  
Optional inputs (related to model parameters):

-lcs_max -lcs_samples "number of samples" -lcs_random -lcs_every_x "skip rate" -min_seq_length "length"

These inpust allow for the user specification of the model parameters as described in Section 4.3.3 of the manuscript.

-lcs_max Sets the model parameter LCS_SAMPLING__NUMBER_OF_RECORDS to max-sampling

-lcs_samples Sets the model parameter LCS_SAMPLING__NUMBER_OF_RECORDS to the number specified

-lcs_random Sets the model parameter LCS_SAMPLING__SELECTION_TYPE_RANDOM to true

-lcs_every_x Sets the model parameter LCS_SAMPLING__SELECTION_TYPE_TAKE_EVERY_X to the number specified

-min_seq_length Sets the model parameter UNCONSTRAINED_ORDER__MINIMUM_SEQUENCE_LENGTH to the length specified"

Optional inputs (related to optional functionality):

-q -o -a

These inputs allow the user to request optional functionality to include post-run record queries and the generation of output files.

-q will invoke a stdout/stdin user interface at the end of the run to enable record value comparisons

-o will output the dataset into the source format (which should match the input file) and a version with row IDs helpful for analysis

-a will output an analysis file with each record prefixed by fields that may aid in analysis

-------
USAGE EXAMPLES:
Below are example command line argument configurations.  Note that the main class is temporal.dedup.TemporalDedup.
  
-d input\\psn_trophy_dataset_obfuscated.txt -t input\\psn_trophy_truth_data.txt -c Game ID -a -s .981

The above example will run TemporalDedup on the PSN dataset, execute the ASNM comparison method with the 'Game ID' blocking key and the .981 similarity threshold, and will output an analysis file.  This will replicate the results presented in Section 6.3 of the manuscript for the PSN data.

-d input\\covid_19_dataset_supplemented.txt -t input\\covid_19_truth_data.txt -c summary_type -a -s .927

The above example will run TemporalDedup on the COVID-19 dataset, execute the ASNM comparison method with the 'summary_type' blocking key and the .927 similarity threshold, and will output an analysis file.  This will replicate the results presented in Section 6.3 of the manuscript for the COVID-19 data.

-d input\\psn_trophy_dataset_obfuscated.txt -t input\\psn_trophy_truth_data.txt -c Game ID -a

The above example will run TemporalDedup on the PSN dataset, execute the ASNM comparison method with the 'Game ID' blocking key and all default similarity thresholds, and will output an analysis file.  This will replicate the results presented in Section 6.2 of the manuscript for the PSN data.

-d input\\covid_19_dataset_supplemented.txt -t input\\covid_19_truth_data.txt -c summary_type -a

The above example will run TemporalDedup on the COVID-19 dataset, execute the ASNM comparison method with the 'summary_type' blocking key and all default similarity thresholds, and will output an analysis file.  This will replicate the results presented in Section 6.2 of the manuscript for the COVID-19 data.
