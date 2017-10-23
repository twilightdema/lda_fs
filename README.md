# lda_fs
Unbreakable LDA Topic Model implementation in pure JAVAScript using LMDB as memory.

Base on LDA JAVAScript implementation of:
https://github.com/primaryobjects/lda

** This implementation assumes balanced distribution of hyper-prior of Dirichlet distribution. 
Dirichlet prior is 0.1 for document-topic and 0.01 for topic-word.

### To install prerequistics:

1) Make sure that you have nodejs with minimum version of 6.x.x installed
2) After clone the project, navigate to project directory and run: 
```
npm install
```

### To run Unit Test:

```
node lda_fs.js unittest
```

### Output sample

Word distribution for each Topic
```
[  
   [  
      {  
         "term":"ธรรมชาติ",
         "probability":0.27
      },
      {  
         "term":"ทำลาย",
         "probability":0.27
      },
      {  
         "term":"ลำธาร",
         "probability":0.22
      }
   ],
   [  
      {  
         "term":"เทคโนโลยี่",
         "probability":0.22
      },
      {  
         "term":"โลก",
         "probability":0.21
      },
      {  
         "term":"แสดงผล",
         "probability":0.11
      }
   ],
   [  
      {  
         "term":"โลก",
         "probability":0.69
      },
      {  
         "term":"ต้นไม้",
         "probability":0.17
      },
      {  
         "term":"เทคโนโลยี่",
         "probability":0.11
      }
   ]
]
```
