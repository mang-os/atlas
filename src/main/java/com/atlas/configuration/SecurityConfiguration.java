package com.atlas.configuration;

import com.atlas.common.ApiErrorWriter;
import jakarta.servlet.DispatcherType;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.web.SecurityFilterChain;

@Configuration(proxyBeanMethods = false)
public class SecurityConfiguration {
    @Bean
    @Profile("!local")
    UserDetailsService disabledIdentity() {
        // Prevent Boot's generated-password user when local identity is not explicitly selected.
        return name -> { throw new UsernameNotFoundException("Local identity is disabled"); };
    }

    @Bean
    SecurityFilterChain security(HttpSecurity http, Environment environment, ApiErrorWriter errors) throws Exception {
        var entryPoint = (org.springframework.security.web.AuthenticationEntryPoint) (request, response, exception) -> {
            response.setHeader("WWW-Authenticate", "Basic realm=\"Atlas local development\"");
            errors.write(request, response, 401, "AUTHENTICATION_REQUIRED", "Valid credentials are required");
        };
        http.sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .requestCache(cache -> cache.disable())
                .csrf(csrf -> csrf.disable())
                .formLogin(form -> form.disable())
                .logout(logout -> logout.disable())
                .exceptionHandling(exceptions -> exceptions.authenticationEntryPoint(entryPoint)
                        .accessDeniedHandler((request, response, exception) ->
                                errors.write(request, response, 403, "FORBIDDEN", "This operation is not permitted")))
                .authorizeHttpRequests(auth -> {
                    auth.dispatcherTypeMatchers(DispatcherType.ERROR).permitAll();
                    auth.requestMatchers(HttpMethod.GET, "/actuator/health").permitAll();
                    auth.requestMatchers("/actuator/**").hasRole("ADMIN");
                    if (environment.acceptsProfiles(Profiles.of("local"))) {
                        auth.requestMatchers(HttpMethod.POST, "/api/resources").hasRole("ADMIN");
                        auth.requestMatchers("/api/**").hasRole("USER");
                    }
                    auth.anyRequest().denyAll();
                });
        if (environment.acceptsProfiles(Profiles.of("local"))) {
            http.httpBasic(basic -> basic.authenticationEntryPoint(entryPoint));
        }
        return http.build();
    }
}
