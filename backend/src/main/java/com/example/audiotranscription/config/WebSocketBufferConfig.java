package com.example.audiotranscription.config;

import org.springframework.boot.web.servlet.ServletContextInitializer;
import org.springframework.context.annotation.Configuration;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.websocket.server.ServerContainer;

@Configuration
public class WebSocketBufferConfig implements ServletContextInitializer {
    @Override
    public void onStartup(ServletContext servletContext) throws ServletException {
        ServerContainer container = (ServerContainer) servletContext.getAttribute("jakarta.websocket.server.ServerContainer");
        if (container != null) {
            // Set both binary and text buffers to 10MB
            int tenMB = 10 * 1024 * 1024; 
            container.setDefaultMaxBinaryMessageBufferSize(tenMB);
            container.setDefaultMaxTextMessageBufferSize(tenMB);
            container.setDefaultMaxSessionIdleTimeout(600000); // 10 minutes
            System.out.println("WebSocket buffer size increased to 10MB");
        }
    }
}