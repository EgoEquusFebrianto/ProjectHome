package kudadiri.data.engineer.security.service;

import kudadiri.data.engineer.domain.entity.User;
import kudadiri.data.engineer.domain.repository.UserRepository;
import kudadiri.data.engineer.security.model.CustomUserDetails;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class CustomUserDetailsService implements UserDetailsService {
    private final UserRepository repository;

    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        User user = repository.findByEmail(username)
                .orElseThrow(
                        () -> new UsernameNotFoundException("User Not Found")
                );

        return new CustomUserDetails(user);
    }
}