package com.atlas.configuration;

import java.util.Collection;
import java.util.UUID;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.User;

/** Immutable identity is attached to verified credentials, never read from an HTTP identity header/body. */
public final class LocalPrincipal extends User {
    private final UUID id;

    public LocalPrincipal(UUID id, String username, String encodedPassword, Collection<? extends GrantedAuthority> authorities) {
        super(username, encodedPassword, authorities);
        this.id = id;
    }

    public UUID id() { return id; }
}
