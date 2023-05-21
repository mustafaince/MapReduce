package demo;
//klasik dosya ve java kütüphaneleri
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
//mapreduce işlemi için gerekli kütüphaneler
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount
{
	static List<String> stopwords= new ArrayList<>();//stopwords leri tuttuğumuz list
	
	public static class task1Mapper extends Mapper<Object, Text, Text, Text> //mapper ın key ve value sünün input ve output parametrelerinin tipleri
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{//map işleminin o anda üzerinde çalışmakta olduğu dosyanın adını alıyoruz
			String filename = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) context.getInputSplit()).getPath().getName();
			
			String line = value.toString();//dosyanın ilk satırı alınıyor
			if(line.length()> 2)
			{
				String[] data = line.split("\t");//tab a göre parçalanıp diziye atılıyor
				
				if(data[0].equals("$PYTWEET$")== true)//eğer ilk kelime pytweet ise;
					line= data[3];//4. parça bizim asıl datamız oluyor. Eğer değilse o satırın hepsi bizim almak istediğimiz datalar oluyor
				line= line.replaceAll("[^a-zA-ZçğıöşüÇĞİÖŞÜ0-9]+", " ");//datadaki istenmeyen karakterleri boşluk karakteriyle değiştiriyoruz
				line= line.toLowerCase();//hepsini küçük harfe çeviriyoruz
				String[] tweets= line.split(" ");//boşluk karakterine göre parçalıyoruz
				String temp="";
				for(int i=0; i<tweets.length; i++) 
				{
					int control =0;
					if(tweets[i].length()> 2) //tüm datayı bir döngüyle dolaşıyoruz
					{
						for(int j=0; j<stopwords.size(); j++) 
						{
							if(tweets[i].equals(stopwords.get(j))==true)//eğer kelime stopwords de varsa o kelimeyi dahil etmiyoruz
							{
								control=1;
								break;
							}
						}
						if(control==0)
							temp+= tweets[i];
						if(i< tweets.length-1 && control==0) 
							temp+= " ";
					}
				}
				String[] words= temp.split(" ");//tekrardan boşluğa göre parçalıyoruz, bu işlemden sonra istediğimiz kelimeleri elde etmiş oluyoruz
				
				for (String word : words) 
				{
					if(word.trim().length()> 2) //uzunluğu 3 den küçük kelimeleri dahil etmiyoruz
					{
						//key i word ve wordun bulunduğu doküman olarak ayarlıyoruz
						Text outputKey = new Text(word+"\t"+filename);
						//value ye 1 değerini veriyoruz. Task1 de amacımız kelimelerin dokümanda kaç kere geçtiğini bulmak
						Text outputValue = new Text("1");
						//ve bu key value değerleriyle mapleme işlemi yapılmış oluyor
						context.write(outputKey, outputValue);
					}
				}
			}
		}
	}
	//task1 reduce un input output tipleri
	public static class task1Reducer extends Reducer<Text, Text, Text, Text> 
	{		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			int sum = 0;
			//her (word ve docname) den oluşan key için reduce fonksiyonu bir kez çalışır
			//bu key e sahip tüm recordların value kısımları values değişkenine atılır.
			//ve tek tek döngüyle dolaşılır
			for (Text val : values)
			{
				//val değerleri kelimenin dosyada toplam ne kadar olduğunu bulmak için toplanır
				sum += Integer.valueOf(val.toString());
			}
			context.write(key, new Text(Integer.toString(sum)));//ve output olarak verilir
		}
	}
	//task2 nin amacı dökümandaki tüm kelimelerin sayısını bulmak
	public static class task2Mapper extends Mapper<Object, Text, Text, Text> 
	{
		int totalOfWords=0;//toplam kelime sayısı

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String line = value.toString();
			String[] lineArray= line.split("\t");//tab a göre okunan ilk record u parçalıyoruz
			String word= lineArray[0];//ilk kelime olan word u aldık
			String docName= lineArray[1];//ikinci kelime olan docname i aldık
			String nValue= lineArray[2];//üçüncü kelime olan n değerini aldık
			//dokümandaki tüm kelimelerin sayısını bulmak için key imizi docname olarak seçip map işlemini yapıyoruz
			context.write(new Text(docName), new Text(word+"\t"+nValue));
		}
	}

	public static class task2Reducer extends Reducer<Text, Text, Text, Text> 
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
	        List<String> tempValues= new ArrayList<>();
			long sum = 0;
			for (Text val : values) 
			{
				//key imiz docName olduğu için values değişkeninin içinde tüm kelimeler bulunuyor
				//amacımız kaç kelime olduğu. bu yüzden sum ın değerini her turda bir kere arttırıyoruz
				//ayrıca değerleri kaybetmemek için tempValues list inde değerlerin yedeğini tutuyoruz
				tempValues.add(val.toString());
				String[] temp= val.toString().split("\t");
				sum += Integer.parseInt(temp[1]);
			}
			String Nvalue= String.valueOf(sum);
			
			for(int i=0; i<tempValues.size(); i++) 
			{
				//list deki tüm değerleri yeniden dolaşıp elde ettiğimiz N(yani dökümandaki kelime sayısını) değerini ekleyip output veriyoruz
				String[] temp= tempValues.get(i).toString().split("\t");
				String nValue= temp[1];
				String word= temp[0];
				//N değerinin eklenmiş şekliyle outputumuz
				context.write(new Text(key), new Text(word+"\t"+ nValue+"\t"+ Nvalue));
			}
		}
	}
	//task 3 deki amacımız verilen kelimenin corpusda toplam kaç dökümanda geçtiğini bulmak.
	//Şu anda elimizde (word, docname) key ine sahip recordlar var. Bizim amacımıza ulaşabilmemiz için key i word olarak seçmemiz gerekir.
	//Böylece o wordun içinde bulunduğu tüm recordların listesini elde etmiş oluruz
	public static class task3Mapper extends Mapper<Object, Text, Text, Text> 
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			//İstediğimiz değerleri elde etmek için gerekli parçalama işlerini yapıyoruz.
			String[] line = value.toString().split("\t");
			String docName= line[0];
			String word= line[1];
			String nValue= line[2];
			String Nvalue= line[3];
			//key i word olarak seçip map işlemini gerçekleştiriyoruz
			context.write(new Text(word), new Text(docName+"\t"+nValue+"\t"+Nvalue));
		}
	}

	public static class task3Reducer extends Reducer<Text, Text, Text, Text> 
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			int sum = 0;
			List<String> tempValues= new ArrayList<>();
			//Reduce fonksiyonu her defasında tek bir key için çalışır. Aynı key e sahip tüm recordların value kısımlarını values değişkenine atar.
			//Eğer values içinde kaç eleman olduğunu bulabilirsek o wordun toplam kaç dökümanda geçtiğini de bulabiliriz.
			for (Text val : values) 
			{
				//Values deki her eleman için bir kez çalışıyor. For döngüsünün toplam kaç kere tur attığını bulabilmek için
				//her turda sum değerini arttırıyoruz.
				tempValues.add(val.toString());//Values iterable ı bir kez dolaşıldığında başa tekrar dönülemiyor. Bu yüzden 
				//recordları bir list de saklıyoruz.
				sum+=1;
			}
			String mValue= Integer.toString(sum);//Sonuçta sum değeri o kelimenin kaç dökümanda geçtiğini göstermiş oluyor.
			
			for(int i=0; i<tempValues.size(); i++) 
			{
				//list den gerekli değerleri çekebilmek için gerekli parçalama işlemleri yapılıyor.
				String[] temp= tempValues.get(i).toString().split("\t");
				String docName= temp[0];
				String nValue= temp[1];
				String Nvalue= temp[2];
				String word= key.toString();
				//sum değişkenindeki değer bizim m değerimizdi. m değerini de ekleyerek diğer tasklar için output veriyoruz.
				context.write(new Text(word), new Text(docName+"\t"+nValue+"\t"+ Nvalue+"\t"+ mValue));
			}
		}
	}
	
	//task 4 son taskımız. Amacı ise kelimelerin her dosya için TF/IDF değerlerini hesaplamak
	public static class task4Mapper extends Mapper<Object, Text, Text, Text> 
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			//gerekli değerleri çekiyoruz
			String[] line = value.toString().split("\t");
			String word= line[0];
			String docName= line[1];
			int nValue= Integer.parseInt(line[2]);
			int Nvalue= Integer.parseInt(line[3]);
			int mValue= Integer.parseInt(line[4]);
			//Belirtilen formülde değişkenleri yerine koyarak tf/idf değerini hesaplıyoruz
			String tf_idf= Double.toString(((double)nValue/Nvalue) * (1+Math.log(19/mValue)));
			//Son olarak yine keyimizi (word, docname) olarak seçip, tf/idf değerini de ekleyerek mapleme işlemi yapıyoruz.
			context.write(new Text(word+"\t"+docName), new Text(tf_idf));
		}
	}

	//Burda aslında map de gereken hesaplamayı yaptığımız için yapacak bir işimiz kalmıyor. Bu yüzden key ve value değerlerini 
	//direk çıktı olarak veriyoruz
	public static class task4Reducer extends Reducer<Text, Text, Text, Text> 
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			for (Text val : values) 
			{
				//Aldığımız değerleri direk output veriyoruz
				context.write(new Text(key), new Text(val));
			}
		}
	}

	public static void main(String[] args) throws Exception 
	{
		File file = new File("stopwords.txt");//stopwords dosyasını istenmeyen kelimeleri çıkarmak için okuyoruz
		FileReader fileReader = new FileReader(file);//klasik dosya okuma işlemleri
		String line;
		BufferedReader br = new BufferedReader(fileReader);
		while ((line = br.readLine()) != null) 
		{
		    stopwords.add(line);//tüm kelimeleri daha sonra kullanmak için bir list e atıyoruz
		}
		br.close();
		fileReader.close();
		
		String inputPath= args[0];//terminalde verilen birinci parametreyi input path i olarak saklıyoruz
		String outputPath= args[1];//ikinci path deki klasörümüz de output için saklanıyor
		//her job için yeni bir configuration ve job instance ı üretiliyor
		Configuration conf1 = new Configuration();
		Job job1 = Job.getInstance(conf1, "task1");//Bu job, üzerinde çalıştığı clusterda task1 ismiyle geçiyor.
		job1.setJar("WordCount.jar");//jar dosyası belirtiliyor
		job1.setJarByClass(WordCount.class);//jar classı belirtiliyor
		job1.setMapperClass(task1Mapper.class);//bu job ı çalıştıracak olan map classı belirtiliyor
		job1.setCombinerClass(task1Reducer.class);//reducer class ı combiner olarak belirtiliyor
		job1.setReducerClass(task1Reducer.class);//aynı zamanda da reducer olarak kullanılıyor
		job1.setOutputKeyClass(Text.class);//Output un key i Text tipinde ayarlanıyor
		job1.setOutputValueClass(Text.class);//output un value sü yine Text tipinde ayarlanıyor
		FileInputFormat.addInputPath(job1, new Path(inputPath));//job için input path i veriliyor
		FileOutputFormat.setOutputPath(job1, new Path(outputPath+"/task1"));//job için output pathi veriliyor
		job1.waitForCompletion(true);//main thread, job1 için çalışmakta olan tüm threadler bitene kadar bekliyor 
		
		Configuration conf2 = new Configuration();//job1 için yapılanların aynısı job 2,3 ve 4 için de yapılıyor
		Job job2 = Job.getInstance(conf2, "task2");
		job2.setJar("WordCount.jar");
		job2.setJarByClass(WordCount.class);
		job2.setMapperClass(task2Mapper.class);
		job2.setCombinerClass(task2Reducer.class);
		job2.setReducerClass(task2Reducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(outputPath+"/task1/part*"));//job1 in outputu job2 için input oluyor
		FileOutputFormat.setOutputPath(job2, new Path(outputPath+"/task2"));
		job2.waitForCompletion(true);
		
		Configuration conf3 = new Configuration();
		Job job3 = Job.getInstance(conf3, "task3");
		job3.setJar("WordCount.jar");
		job3.setJarByClass(WordCount.class);
		job3.setMapperClass(task3Mapper.class);
		job3.setCombinerClass(task3Reducer.class);
		job3.setReducerClass(task3Reducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job3, new Path(outputPath+"/task2/part*"));//job2 nin outputu job3 için input oluyor
		FileOutputFormat.setOutputPath(job3, new Path(outputPath+"/task3"));
		job3.waitForCompletion(true);
		
		Configuration conf4 = new Configuration();
		Job job4 = Job.getInstance(conf4, "task4");
		job4.setJar("WordCount.jar");
		job4.setJarByClass(WordCount.class);
		job4.setMapperClass(task4Mapper.class);
		job4.setCombinerClass(task4Reducer.class);
		job4.setReducerClass(task4Reducer.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job4, new Path(outputPath+"/task3/part*"));//job3 ün outputu job4 için input oluyor
		FileOutputFormat.setOutputPath(job4, new Path(outputPath+"/task4"));
		
		System.exit(job4.waitForCompletion(true) ? 0 : 1);//en son program job4 bitince sonlanıyor
	}
}



















