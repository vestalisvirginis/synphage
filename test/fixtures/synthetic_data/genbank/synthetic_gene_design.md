Synthetic gene design for synphage testing:


| file_name    |  features  | comment                                  | nbr_locus | locus_1 | locus_1_note | locus_2    | locus_2_note | locus_3    | locus_3_note | locus_4    | locus_4_note | locus_5      | locus_5_note | locus_6     | locus_6_note | locus_7      | locus_7_note |
------------------------------------------------------------------------------------------------------
| TT_000001.gb | gene       | ref seq                                  |    5      | 1..1000 | L1_ref       | 1001..3000 | L2_ref       | 3001..4000 | L3_ref       | 4001..7000 | L4_ref       | 7001..10000 | L5_ref        |             |              |              |              |
| TT_000002.gb | gene       | gene id decrease + split gene + new gene |    7      | 1..1000 | L1           | 1001..3000 | L2           | 3001..4000 | L3_m12       | 4001..7000 | L4           | 7001..9000  | L5_t          | 9001..10000 | L5t+m200     | 10001..11000 | new_gene     |  
| TT_000003.gb | gene       | gene id decrease                         |    5      | 1..1000 | L1           | 1001..3000 | L2_m100      | 3001..4000 | L3_phage2    | 4001..7000 | L4_m200      | 7001..10000 | L5            |             |              |              |              |        
| TT_000004.gb | gene       | inverted seq                             |    5      | 1..3000 | L5           | 3001..6000 | L4_m         | 6001..7000 | L3           | 7001..9000 | L2           | 9001..10000 | L1_m          |             |              |              |              |
| TT_000005.gb | gene       | all gene different                       |    7      | 1..1000 | diff.        | 1001..3000 | diff.        | 3001..6000 | diff.        | 6001..7000 | diff.        | 7001..8000  | diff.         | 8001..9000  | diff.        | 9001..10000  | diff.        |
| TT_000006.gb | CDS        | identical to phage 1                     |    5      | 1..1000 | L1           | 1001..3000 | L2           | 3001..4000 | L3           | 4001..7000 | L4           | 7001..10000 | L5            |           |            |         |              |

t: truncated gene
m: modified nt
d: deleted nt
a: added nt



Expected results:
