package com.atlas.configuration;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

@Configuration(proxyBeanMethods = false)
@Profile("local")
public class LocalIdentityConfiguration {
    public static final UUID ALICE_ID = UUID.fromString("11111111-1111-4111-8111-111111111111");
    public static final UUID BOB_ID = UUID.fromString("22222222-2222-4222-8222-222222222222");
    public static final UUID ADMIN_ID = UUID.fromString("33333333-3333-4333-8333-333333333333");

    @Bean
    PasswordEncoder localPasswordEncoder() { return new BCryptPasswordEncoder(10); }

    @Bean
    UserDetailsService localUsers(Environment environment, PasswordEncoder encoder) {
        String alice = encoder.encode(requiredPassword(environment, "alice"));
        String bob = encoder.encode(requiredPassword(environment, "bob"));
        String admin = encoder.encode(requiredPassword(environment, "admin"));
        // A fresh UserDetails per request prevents credential erasure from modifying the stored hashes.
        return username -> switch (username) {
            case "alice" -> principal(ALICE_ID, "alice", alice, false);
            case "bob" -> principal(BOB_ID, "bob", bob, false);
            case "admin" -> principal(ADMIN_ID, "admin", admin, true);
            default -> throw new UsernameNotFoundException("Unknown local principal");
        };
    }

    private String requiredPassword(Environment environment, String username) {
        String key = "atlas.local." + username + "-password";
        String password = environment.getProperty(key);
        if (password == null || password.isBlank() || password.getBytes(StandardCharsets.UTF_8).length > 72) {
            throw new IllegalStateException("Set a nonblank local credential of at most 72 UTF-8 bytes: " + key);
        }
        return password;
    }

    private LocalPrincipal principal(UUID id, String name, String hash, boolean admin) {
        var roles = admin ? List.of(new SimpleGrantedAuthority("ROLE_USER"), new SimpleGrantedAuthority("ROLE_ADMIN"))
                : List.of(new SimpleGrantedAuthority("ROLE_USER"));
        return new LocalPrincipal(id, name, hash, roles);
    }
}
