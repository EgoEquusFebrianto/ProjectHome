// Spring Boot adalah kerangka kerja yang menyederhanakan pengembangan aplikasi berbasis Spring,
// sehingga memudahkan dan mempercepat proses pembuatan aplikasi web dan layanan mikro.

// Penjelasan Annotations yang digunakan
// @SpringBootApplication: Anotasi utama yang menggabungkan @Configuration, @EnableAutoConfiguration, dan @ComponentScan. Digunakan di class utama.
// @Autowired: untuk menyuntikkan (inject) dependensi secara otomatis dari Spring Container

// Implements CommandLineRunner: Interface untuk menjalankan kode saat aplikasi Spring Boot Start (seperti main)


package engineering.spring_pemula.core;

import engineering.spring_pemula.core.application.Fire;
import engineering.spring_pemula.core.springapplicationevent.StartingListener;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class LearningSpringApplication {

	@Bean
	public Fire fire() {
		return new Fire();
	}

//	public static void main(String[] args) {
//		ConfigurableApplicationContext context = SpringApplication.run(LearningSpringApplication.class, args);
//
//		Fire variable = context.getBean(Fire.class);
//		System.out.println(variable);
//	}

//	public static void main(String[] args) {
//		SpringApplication application = new SpringApplication(LearningSpringApplication.class);
//		application.setBannerMode(Banner.Mode.OFF);
//
//		application.addListeners(new StartingListener());
//
//		application.run(args);
//	}


	// Banner Shutdown
//	public static void main(String[] args) {
//		SpringApplication application = new SpringApplication(LearningSpringApplication.class);
//		application.setBannerMode(Banner.Mode.OFF);
//
//		ConfigurableApplicationContext appContext = application.run(args);
//
//		Fire fire = appContext.getBean(Fire.class);
//		System.out.println(fire);
//	}

	// ApplicationRunner

	public static void main(String[] args) {
		SpringApplication.run(LearningSpringApplication.class, args);

	}

}

//import engineering.spring_pemula.core.introductionIoC.GreetingPrinter;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.CommandLineRunner;
//
//@SpringBootApplication
//public class LearningSpringApplication implements CommandLineRunner {
//
//	@Autowired
//	private GreetingPrinter greetingPrinter;
//
//	public static void main(String[] args) {
//		SpringApplication.run(LearningSpringApplication.class, args);
//	}
//
//	@Override
//	public void run(String... args) {
//		greetingPrinter.printGreeting();
//	}
//}

// Contoh implementasi variable: java -jar app.jar arg1 arg2

