package kudadiri.data.engineer.service;

import jakarta.persistence.EntityNotFoundException;
import kudadiri.data.engineer.domain.entity.Role;
import kudadiri.data.engineer.domain.entity.User;
import kudadiri.data.engineer.domain.enums.UserStatus;
import kudadiri.data.engineer.domain.repository.RoleRepository;
import kudadiri.data.engineer.domain.repository.UserRepository;
import kudadiri.data.engineer.dto.request.LoginRequest;
import kudadiri.data.engineer.dto.request.RegisterRequest;
import kudadiri.data.engineer.dto.response.AuthResponse;
import kudadiri.data.engineer.dto.response.UserResponse;
import kudadiri.data.engineer.security.jwt.JwtService;
import kudadiri.data.engineer.security.model.CustomUserDetails;
import kudadiri.data.engineer.utils.RoleConstant;
import lombok.RequiredArgsConstructor;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Transactional
public class AuthService {
    private final UserRepository userRepository;
    private final RoleRepository roleRepository;
    private final PasswordEncoder passwordEncoder;
    private final JwtService jwtService;
    private final AuthenticationManager authenticationManager;

    public AuthResponse register (RegisterRequest request) {
        if (userRepository.existsByEmail(request.getEmail())) {
            throw new IllegalArgumentException("Email Already Exists");
        }

        Role role = roleRepository.findById(RoleConstant.CUSTOMER).orElseThrow(
                () -> new EntityNotFoundException("Role Not Found.")
        );

        User user = new User();

        user.setFullName(request.getFullName());
        user.setEmail(request.getEmail());
        user.setPhone(request.getPhone());
        user.setPassword(
                passwordEncoder.encode(request.getPassword())
        );
        user.setRole(role);
        user.setStatus(UserStatus.ACTIVE);

        userRepository.save(user);

        String token = jwtService.generateToken(
                new CustomUserDetails(user)
        );

        return AuthResponse.builder()
                .accessToken(token)
                .user(toUserResponse(user))
                .build();
    }

    public AuthResponse login(LoginRequest request) {
        authenticationManager.authenticate(new UsernamePasswordAuthenticationToken(
                request.getEmail(),
                request.getPassword()
        ));

        User user = userRepository.findByEmail(request.getEmail()).orElseThrow(
                () -> new UsernameNotFoundException("USer Not Found")
        );

        String token = jwtService.generateToken(new CustomUserDetails(user));

        return AuthResponse.builder()
                .accessToken(token)
                .user(toUserResponse(user))
                .build();
    }

    private UserResponse toUserResponse(User user) {
        return UserResponse.builder()
                .id(user.getId())
                .fullName(user.getFullName())
                .email(user.getEmail())
                .phone(user.getPhone())
                .role(user.getRole().getRoleName())
                .build();
    }
}
