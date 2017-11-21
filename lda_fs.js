var stem = require('stem-porter');
var fs = require('fs');
var Q = require('q');
var util = require('util');
var stream = require('stream');
//var es = require('event-stream');

var lmdb = require('node-lmdb');

///////////////////////////////////////////////////////////////////
// Utility function to manipulate LMDB as multi-dimentional array
function LMDBArray(guid, env, name, openExisting = false) {
    this.env = env;
    this.name = name;
    this.autoCommit = true;
    this.txn = null;
    this.dbi = this.env.openDbi({
        name: name,
        create: true // will create if database did not exist
    });
    if(openExisting === true) {
        let txn = this.env.beginTxn();
        txn.putNumber(this.dbi, 'length', 0);
        txn.commit();
    }
}
LMDBArray.prototype.setTransaction = function(txn) {
    this.txn = txn;
};
LMDBArray.prototype.dispose = function() {
    this.dbi.close();
};
LMDBArray.prototype.delete = function() {
    this.dbi.drop();
};
LMDBArray.prototype.push = function(value, dimensions) {
    if(typeof dimensions === 'undefined')
        dimensions = [];
    let index_key = dimensions.reduce((prev, cur)=>{
        return prev + '_' + String(cur);
    }, '');
    let length_key = 'length' + index_key;    
    let txn = (this.autoCommit)?this.env.beginTxn():this.txn;
    let ext_length = txn.getNumber(this.dbi, length_key);
    if(ext_length === null)
        ext_length = 0;
    //console.log(this.name+'::'+'Getting length from: '+length_key+' => '+ext_length);
    txn.putNumber(this.dbi, length_key, ext_length + 1);    
    let value_key = index_key + '_' + ext_length;
    //console.log(this.name+'::'+'Push -> '+value_key+': '+value);
    txn.putString(this.dbi, value_key, value);        
    if(this.autoCommit) txn.commit();
};
LMDBArray.prototype.length = function(dimensions) {
    if(typeof dimensions === 'undefined')
        dimensions = [];
    let index_key = dimensions.reduce((prev, cur)=>{
        return prev + '_' + String(cur);
    }, '');
    let length_key = 'length' + index_key;
    let txn = (this.autoCommit)?this.env.beginTxn():this.txn;
    let ext_length = txn.getNumber(this.dbi, length_key);
    if(ext_length === null)
        ext_length = 0;
    //console.log(this.name+'::'+'Getting length from: '+length_key+' => '+ext_length);
    if(this.autoCommit) txn.commit();
    return ext_length;
};
LMDBArray.prototype.indexOf = function(searchValue, dimensions) {
    if(typeof dimensions === 'undefined')
        dimensions = [];
    let index_key = dimensions.reduce((prev, cur)=>{
        return prev + '_' + String(cur);
    }, '');
    let length_key = 'length' + index_key;
    let txn = (this.autoCommit)?this.env.beginTxn():this.txn;
    let ext_length = txn.getNumber(this.dbi, length_key);
    var pos = -1;
    for(var i=0;i<ext_length;i++) {
        let value_key = index_key + '_' + i;
        //console.log(this.name+'::'+'Get <- '+value_key);
        let value = txn.getString(this.dbi, value_key);
        //console.log(this.name+'::'+'Get <- '+value_key+': '+value);            
        if(value === searchValue) {
            pos = i;
            break;
        } // if
    } // for i    
    if(this.autoCommit) txn.commit();
    return pos;
};
LMDBArray.prototype.get = function(index, dimensions) {
    if(typeof dimensions === 'undefined')
        dimensions = [];
    let index_key = dimensions.reduce((prev, cur)=>{
        return prev + '_' + String(cur);
    }, '');
    let txn = (this.autoCommit)?this.env.beginTxn():this.txn;
    let value_key = index_key + '_' + index;
    //console.log(this.name+'::'+'Get <- '+value_key);
    let value = txn.getString(this.dbi, value_key);
    //console.log(this.name+'::'+'Get <- '+value_key+': '+value);
    if(this.autoCommit) txn.commit();
    return value;
};
LMDBArray.prototype.put = function(value, index, dimensions) {
    if(typeof dimensions === 'undefined')
        dimensions = [];
    let index_key = dimensions.reduce((prev, cur)=>{
        return prev + '_' + String(cur);
    }, '');
    let txn = (this.autoCommit)?this.env.beginTxn():this.txn;
    let value_key = index_key + '_' + index;
    //console.log(this.name+'::'+'Put/Set -> '+value_key+': '+value);
    txn.putString(this.dbi, value_key, value);        
    if(this.autoCommit) txn.commit();
};
LMDBArray.prototype.increment = function(incrementValue, index, dimensions) {
    let oldVal = this.get(index, dimensions);
    let newVal = parseInt(oldVal) + incrementValue;
    this.put(String(newVal), index, dimensions);
};

LMDBArray.prototype.set = LMDBArray.prototype.put;

//
// Based on javascript implementation https://github.com/awaisathar/lda.js
// Original code based on http://www.arbylon.net/projects/LdaGibbsSampler.java
//
var process__ = function(sentences, numberOfTopics, numberOfTermsPerTopic, languages, alphaValue, betaValue, randomSeed) {

    var guid = function() {
        var s4 = function() {
            return Math.floor((1 + Math.random()) * 0x10000)
            .toString(16)
            .substring(1);
        }
    return s4() + s4() + '-' + s4() + '-' + s4() + '-' +
        s4() + '-' + s4() + s4() + s4();
    };
        
    GUID = guid();
    
    if (!fs.existsSync('lda_temp')){
        fs.mkdirSync('lda_temp');
    } // if
    if (!fs.existsSync('lda_temp/'+GUID)){
        fs.mkdirSync('lda_temp/'+GUID);
    } // if
    
    console.log('Initialize LMDB...');
    var env = new lmdb.Env();
    env.open({
        path: 'lda_temp/'+GUID,
        mapSize: 2*1024*1024*1024, // maximum database size
        maxDbs: 9999
    });

    var config = new LMDBArray(GUID, env, 'config');
    // Index-encoded array of sentences, with each row containing the indices of the words in the vocabulary.
    var documents = new LMDBArray(GUID, env, 'documents');
    // Hash of vocabulary words and the count of how many times each word has been seen.
    var f = new LMDBArray(GUID, env, 'f');
    // Vocabulary of unique words (porter stemmed).
    var vocab = new LMDBArray(GUID, env, 'vocab');
    // Vocabulary of unique words in their original form.
    var vocabOrig = new LMDBArray(GUID, env, 'vocabOrig');
    
    // ----------
          
    // The result will consist of topics and their included terms [[{"term":"word1", "probability":0.065}, {"term":"word2", "probability":0.047}, ... ], [{"term":"word1", "probability":0.085}, {"term":"word2", "probability":0.024}, ... ]].
    var result = {};
    // Array of stop words
    languages = languages; //  || Array('en'); Nok: no Default!

    if (sentences && sentences.length > 0) {
      var stopwords = new Array();

      languages.forEach(function(value) {
          var stopwordsLang = require('./stopwords_' + value + ".js");
          stopwords = stopwords.concat(stopwordsLang.stop_words);
      });


      for(var i=0;i<sentences.length;i++) {
          if (sentences[i]=="") continue;
          documents.push(JSON.stringify([]));

          var words = sentences[i].split(/[\s,\"]+/);
          console.log('words = ' +JSON.stringify(words));

          if(!words) continue;
          for(var wc=0;wc<words.length;wc++) {
              var w = words[wc].toLowerCase();
              if(languages.indexOf('en') != -1)
                  var w=w.replace(/[^a-z\'A-Z0-9\u00C0-\u00ff ]+/g, '');
              var wStemmed = stem(w);
              //console.log('wStemmed = ' +JSON.stringify(wStemmed));

              if (w=="" || !wStemmed || w.length==1 || stopwords.indexOf(w.replace("'", "")) > -1 || stopwords.indexOf(wStemmed) > -1 || w.indexOf("http")==0) continue;
              let f_stemmed = f.get(wStemmed);
              if (f_stemmed != null) {
                  let f_stemmed_updated = parseInt(f_stemmed) + 1;
                  f.put(String(f_stemmed_updated), wStemmed);
              } 
              else if(wStemmed) { 
                    f.put(String(1), wStemmed);
                    vocab.push(wStemmed);
                    vocabOrig.put(w, wStemmed);
              };
              
              documents.push(String(vocab.indexOf(wStemmed)), [i]);
          }
      }
          
      var V = vocab.length();
      var M = documents.length();
      var K = parseInt(numberOfTopics);
      var alpha = alphaValue || 0.1;  // per-document distributions over topics
      var beta = betaValue || .01;  // per-topic distributions over words
      
      console.log('V (vocab).length = '+V);
      console.log('M (document).length = '+M);
      
      lda.configure(GUID, env, documents,V, 10, 0, 0, 0, /*randomSeed*/1);
      lda.gibbs(K, alpha, beta);

      var theta = lda.getTheta();
      var phi = lda.getPhi();

      result.topicModel = {};
      
      result.topicModel.hypers = {};
      result.topicModel.hypers.W = V;
      result.topicModel.hypers.T = K;
      result.topicModel.hypers.vocab = vocab;

      result.topicModel.priors = {};
      result.topicModel.priors.alpha = alpha;
      result.topicModel.priors.beta = beta;

      result.topicModel.posteriors = {};
      result.topicModel.posteriors.theta = theta;
      result.topicModel.posteriors.phi = phi;
      
      result.topicModel.counters = {};
      result.topicModel.counters.nw = {type:'lmdb', name:'nw'};
      result.topicModel.counters.nd = {type:'lmdb', name:'nd'};
      result.topicModel.counters.nwsum = {type:'lmdb', name:'nwsum'};
      result.topicModel.counters.ndsum = {type:'lmdb', name:'ndsum'};
      
      result.printReadableOutput = function() {
        
        var _env = new lmdb.Env();
        _env.open({
            path: 'lda_temp/'+GUID,
            mapSize: 2*1024*1024*1024, // maximum database size
            maxDbs: 9999
        });

        // Vocabulary of unique words (porter stemmed).
        var _vocab = new LMDBArray(GUID, _env, 'vocab', true);
        // Vocabulary of unique words in their original form.
        var _vocabOrig = new LMDBArray(GUID, _env, 'vocabOrig', true);
        
        // TODO: May output to string instead
        // var text = '';

        //topics
        console.log('=Topic Distribution=');
        var topTerms=numberOfTermsPerTopic;
        for (var k = 0; k < phi.length; k++) {
            var things = new Array();
            console.log('Topic ' + (k + 1));
            for (var w = 0; w < phi[k].length; w++) {
                let vocab_w = _vocab.get(w);
                things.push(""+phi[k][w].toPrecision(2)+"_"+vocab_w + "_" + _vocabOrig.get(vocab_w));
            }
            things.sort().reverse();
            if(topTerms>vocab.length) topTerms=vocab.length;

            for (var t = 0; t < topTerms; t++) {
                var topicTerm=things[t].split("_")[2];
                var prob=parseInt(things[t].split("_")[0]*100);
                if (prob<2) continue;
                console.log(topicTerm + ' (' + prob + '%)');              
            }
        }

        _env.close();        
      };
    }

    config.dispose();
    documents.dispose();
    f.dispose();
    vocab.dispose();
    vocabOrig.dispose();

    env.close();        
    
    return result;
}

function makeArray(guid, env, name, x, openExisting = false) {
    let a = new LMDBArray(guid, env, name, openExisting);
    if(openExisting == false) {
        for(var i=0;i<x;i++)  {
            a.push(String(0));
        } // for
    }
    return a;
}

function make2DArray(guid, env, name, x, y, openExisting = false) {
    let a = new LMDBArray(guid, env, name, openExisting);
    if(openExisting == false) {
        for(var i=0;i<x;i++)  {
            a.push(JSON.stringify([]));
            for (var j=0;j<y;j++) {
                a.push(String(0), [i]);
            }
        }
    }
    return a;
}

var lda = new function() {
    var documents,z,nw,nd,nwsum,ndsum,thetasum,phisum,V,K,alpha,beta; 
    var THIN_INTERVAL = 20;
    var BURN_IN = 100;
    var ITERATIONS = 5000;
    var SAMPLE_LAG;
    var RANDOM_SEED;
    var dispcol = 0;
    var numstats=0;
    var guid = null;
    var env = null;

    this.configure = function (guid, env, docs,v,iterations,burnIn,thinInterval,sampleLag,randomSeed) {
        this.guid = guid;
        this.env = env;
        this.ITERATIONS = iterations;
        this.BURN_IN = burnIn;
        this.THIN_INTERVAL = thinInterval;
        this.SAMPLE_LAG = sampleLag;
        this.RANDOM_SEED = randomSeed;
        this.documents = docs;
        this.V = v;
        this.dispcol=0;
        this.numstats=0; 
        this.M = 0;
    }
    this.initialState = function (K) {
        var i;
        this.M = this.documents.length();
        console.log('M = ' + this.M);
        this.nw = make2DArray(this.guid, this.env, 'nw', this.V,K); 
        this.nd = make2DArray(this.guid, this.env, 'nd', this.M,K); 
        this.nwsum = makeArray(this.guid, this.env, 'nwsum', K); 
        this.ndsum = makeArray(this.guid, this.env, 'ndsum', this.M);
        this.z = new LMDBArray(this.guid, this.env, 'z');   
        for (i=0;i<this.M;i++) 
            this.z.push(JSON.stringify([]));
        for (var m = 0; m < this.M; m++) {
            var N = this.documents.length([m]);
            console.log('N documents['+m+'].length = ' + N);
            for (var n = 0; n < N; n++) {
                var topic = parseInt(""+(this.getRandom() * K));   
                this.z.push(String(topic),[m]);
                let w = this.documents.get(n, [m]);
                this.nw.increment(1, topic, [parseInt(w)] );
                this.nd.increment(1, topic, [m] );
                this.nwsum.increment(1, topic);
            }
            this.ndsum.set(String(N), m);
        }
    }
    
    this.gibbs = function (K,alpha,beta) {
        var i;
        this.K = K;
        this.alpha = alpha;
        this.beta = beta;
        if (this.SAMPLE_LAG > 0) {
            this.thetasum = make2DArray(this.guid, this.env, 'thetasum', this.M,this.K);
            this.phisum = make2DArray(this.guid, this.env, 'phisum', this.K,this.V);
            this.numstats = 0;
        }
        this.initialState(K);
        //document.write("Sampling " + this.ITERATIONS
         //   + " iterations with burn-in of " + this.BURN_IN + " (B/S="
         //   + this.THIN_INTERVAL + ").<br/>");
         for (i = 0; i < this.ITERATIONS; i++) {
            console.log('ITERATIONS: '+i);
            for (var m = 0; m < this.z.length(); m++) {
                for (var n = 0; n < this.z.length([m]); n++) {
                    var topic = this.sampleFullConditional(m, n);
                    this.z.set(String(topic),n,[m]);
                }
            }
            if ((i < this.BURN_IN) && (i % this.THIN_INTERVAL == 0)) {
                //document.write("B");
                this.dispcol++;
            }
            if ((i > this.BURN_IN) && (i % this.THIN_INTERVAL == 0)) {
                //document.write("S");
                this.dispcol++;
            }
            if ((i > this.BURN_IN) && (this.SAMPLE_LAG > 0) && (i % this.SAMPLE_LAG == 0)) {
                this.updateParams();
                //document.write("|");                
                if (i % this.THIN_INTERVAL != 0)
                    this.dispcol++;
            }
            if (this.dispcol >= 100) {
                //document.write("*<br/>");                
                this.dispcol = 0;
            }
        }       
    }
    
    this.sampleFullConditional = function(m,n) {
        var topic = parseInt(this.z.get(n, [m]));
        let w = parseInt(this.documents.get(n, [m]));
        this.nw.increment(-1, topic, [w]);
        this.nd.increment(-1, topic, [m]);
        this.nwsum.increment(-1, topic);
        this.ndsum.increment(-1, m);

        var p = makeArray(this.guid, this.env, 'p', this.K);
        for (var k = 0; k < this.K; k++) {
            let p_k = (parseInt(this.nw.get(k,[w])) + this.beta) / (parseInt(this.nwsum.get(k)) + this.V * this.beta)
            * (parseInt(this.nd.get(k,[m])) + this.alpha) / (parseInt(this.ndsum.get(m)) + this.K * this.alpha);    
            p.set(String(p_k), k);
        }
        for (var k = 1; k < p.length(); k++) {
            let p_k_m_1 = parseFloat(p.get(k - 1));
            let p_k = parseFloat(p.get(k));
            p.set(String(p_k + p_k_m_1), k);
        }
        var u = this.getRandom() * parseFloat(p.get(this.K - 1));
        for (topic = 0; topic < p.length(); topic++) {
            let p_topic = parseFloat(p.get(topic));
            if (u < p_topic)
                break;
        }
        this.nw.increment(1, topic, [w]);
        this.nd.increment(1, topic, [m]);
        this.nwsum.increment(1, topic);
        this.ndsum.increment(1, m);
        p.delete();
        return topic;
    }
    
    this.updateParams =function () {
        for (var m = 0; m < this.M; m++) {
            for (var k = 0; k < this.K; k++) {
                let thethasum_ext = parseFloat(this.thetasum.get(k,[m]));
                let thethasum_new = thethasum_ext + (parseInt(this.nd.get(k,[m])) + this.alpha) / (parseInt(this.ndsum.get(m)) + this.K * this.alpha);
                this.thetasum.set(String(thethasum_new),k,[m]);
            }
        }
        for (var k = 0; k < this.K; k++) {
            for (var w = 0; w < this.V; w++) {
                let phisum_ext = parseFloat(this.phisum.get(w,[k]));
                let phisum_new = phisum_ext + (parseInt(this.nw.get(k,[w])) + this.beta) / (parseInt(this.nwsum.get(k)) + this.V * this.beta);
                this.phisum.set(String(phisum_new),w,[k]);
            }
        }
        this.numstats++;
    }
    
    this.getTheta = function() {
        var theta = new Array(); 
        for(var i=0;i<this.M;i++) theta[i] = new Array();
        if (this.SAMPLE_LAG > 0) {
            for (var m = 0; m < this.M; m++) {
                for (var k = 0; k < this.K; k++) {
                    theta[m][k] = parseFloat(this.thetasum.get(k,[m])) / this.numstats;
                }
            }
        } else {
            for (var m = 0; m < this.M; m++) {
                for (var k = 0; k < this.K; k++) {
                    theta[m][k] = (parseFloat(this.nd.get(k,[m])) + this.alpha) / (parseFloat(this.ndsum.get(m)) + this.K * this.alpha);
                }
            }
        }
        return theta;
    }
    
    this.getPhi = function () {
        var phi = new Array(); for(var i=0;i<this.K;i++) phi[i] = new Array();
        if (this.SAMPLE_LAG > 0) {
            for (var k = 0; k < this.K; k++) {
                for (var w = 0; w < this.V; w++) {
                    phi[k][w] = parseFloat(this.phisum.get(w,[k])) / this.numstats;
                }
            }
        } else {
            for (var k = 0; k < this.K; k++) {
                for (var w = 0; w < this.V; w++) {
                    phi[k][w] = (parseFloat(this.nw.get(k,[w])) + this.beta) / (parseFloat(this.nwsum.get(k)) + this.V * this.beta);
                }
            }
        }
        return phi;
    }

    this.getRandom = function() {
        if (this.RANDOM_SEED) {
            // generate a pseudo-random number using a seed to ensure reproducable results.
            var x = Math.sin(this.RANDOM_SEED++) * 1000000;
            return x - Math.floor(x);
        } else {
            // use standard random algorithm.
            return Math.random();
        }
    }
}

module.exports = process__;

if(process.argv.length >= 3 && process.argv[2]==='unittest') {
    // Unit testing function
    function unitTest_LDA() {
        var sentences = [
            'คอมพิวเตอร์ เทคโนโลยี่ โลก แสดงผล',
            'โลก ต้นไม้ ธรรมชาติ ลำธาร เทคโนโลยี่ ทำลาย',
            'เทคโนโลยี่ โลก ธรรมมะ หลุดพ้น ดับทุกข์',
            'คอมพิวเตอร์ เทคโนโลยี่ โลก แสดงผล',
            'โลก ต้นไม้ ธรรมชาติ ลำธาร เทคโนโลยี่ ทำลาย',
            'เทคโนโลยี่ โลก ธรรมมะ หลุดพ้น ดับทุกข์',
            'คอมพิวเตอร์ เทคโนโลยี่ โลก แสดงผล',
            'โลก ต้นไม้ ธรรมชาติ ลำธาร เทคโนโลยี่ ทำลาย',
            'เทคโนโลยี่ โลก ธรรมมะ หลุดพ้น ดับทุกข์',
            'คอมพิวเตอร์ เทคโนโลยี่ โลก แสดงผล',
            'โลก ต้นไม้ ธรรมชาติ ลำธาร เทคโนโลยี่ ทำลาย',
            'เทคโนโลยี่ โลก ธรรมมะ หลุดพ้น ดับทุกข์',
            'คอมพิวเตอร์ เทคโนโลยี่ โลก แสดงผล',
            'โลก ต้นไม้ ธรรมชาติ ลำธาร เทคโนโลยี่ ทำลาย',
            'เทคโนโลยี่ โลก ธรรมมะ หลุดพ้น ดับทุกข์',
        ];
        var result = process__(sentences, 3, 3, ['th']);
        console.log(JSON.stringify(result.topicModel));
        result.printReadableOutput();
    } 
    unitTest_LDA();
}

