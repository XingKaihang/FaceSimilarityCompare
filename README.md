# FaceSimilarityCompare
The Environment and deploy method is quite similar to https://github.com/XingKaihang/ImportImages

* Aim: supose we have a HIB in hdfs, which contains many face images. And given an ordinary person's face image, this project will find out the most similarity image in HIB.

* Usage: (under hipi root directory) use command: hadoop jar facedetec/facedetec.jar [HIB NAME] [OUTPUT DIR]

* Attention: we have hard code the image path, temporary directory, you need to adjust some code based on your requirements.
