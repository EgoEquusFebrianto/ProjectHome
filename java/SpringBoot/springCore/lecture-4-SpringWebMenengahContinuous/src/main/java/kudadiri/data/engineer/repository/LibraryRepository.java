package kudadiri.data.engineer.repository;

import kudadiri.data.engineer.entity.Book;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface LibraryRepository extends JpaRepository<Book, Long> {
    List<Book> findBookByAuthor(String author);
}