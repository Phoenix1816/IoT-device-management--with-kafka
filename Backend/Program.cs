using Backend.BackgroundServices;
using Backend.Data;
using Backend.Hubs;
using Backend.Models;
using Backend.Services;
using Backend.Services.Interfaces;

using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore;
using Microsoft.IdentityModel.Tokens;

using System.Text;

var builder = WebApplication.CreateBuilder(args);


// KAFKA SERVICE

builder.Services.AddHostedService<KafkaConsumerService>();

builder.Services.AddSingleton<IKafkaProducerService, KafkaProducerService>();


// TELEMETRY INGESTION SERVICE

builder.Services.AddScoped<
    ITelemetryIngestionService,
    SimulationTelemetryIngestionService
>();


// EMAIL SERVICE

builder.Services.AddScoped<
    IEmailService,
    EmailService
>();


// OPEN-METEO

builder.Services.AddHttpClient<
    OpenMeteoService
>();


// CONTROLLERS

builder.Services.AddControllers();

builder.Services.AddHttpContextAccessor();


// AUDIT LOG

builder.Services.AddScoped<
    AuditLogService
>();


// JWT AUTHENTICATION

builder.Services
    .AddAuthentication(
        JwtBearerDefaults.AuthenticationScheme
    )
    .AddJwtBearer(options =>
    {
        options.TokenValidationParameters =
            new TokenValidationParameters
            {
                ValidateIssuer = true,
                ValidateAudience = true,
                ValidateLifetime = true,
                ValidateIssuerSigningKey = true,

                ValidIssuer =
                    builder.Configuration["Jwt:Issuer"],

                ValidAudience =
                    builder.Configuration["Jwt:Audience"],

                IssuerSigningKey =
                    new SymmetricSecurityKey(
                        Encoding.UTF8.GetBytes(
                            builder.Configuration["Jwt:Key"]!
                        )
                    )
            };
    });


// PASSWORD HASHER

builder.Services.AddScoped<
    IPasswordHasher<User>,
    PasswordHasher<User>
>();

// JWT SERVICE

builder.Services.AddScoped<
    IJwtService,
    JwtService
>();


// CORS

builder.Services.AddCors(options =>
{
    options.AddPolicy(
        "Frontend",
        policy =>
        {
            policy
                .WithOrigins(
                    "http://localhost:5173"
                )
                .AllowAnyHeader()
                .AllowAnyMethod()
                .AllowCredentials();
        }
    );
});


// SIGNALR

builder.Services.AddSignalR();


// DATABASE

builder.Services.AddDbContext<AppDbContext>(
    options =>
        options.UseMySql(
            builder.Configuration.GetConnectionString(
                "DefaultConnection"
            ),
            ServerVersion.AutoDetect(
                builder.Configuration.GetConnectionString(
                    "DefaultConnection"
                )
            )
        )
);


// TELEMETRY SIMULATION WORKER

builder.Services.AddHostedService<
    TelemetrySimulationWorker
>();

// SWAGGER

builder.Services.AddEndpointsApiExplorer();

builder.Services.AddSwaggerGen(
    options =>
    {
        options.AddSecurityDefinition(
            "Bearer",
            new Microsoft.OpenApi.Models.OpenApiSecurityScheme
            {
                Name = "Authorization",

                Type =
                    Microsoft.OpenApi.Models.SecuritySchemeType.Http,

                Scheme = "bearer",

                BearerFormat = "JWT",

                In =
                    Microsoft.OpenApi.Models.ParameterLocation.Header,

                Description =
                    "JWT token giriniz. Örnek: Bearer eyJhbGciOiJIUzI1NiIs..."
            }
        );

        options.AddSecurityRequirement(
            new Microsoft.OpenApi.Models.OpenApiSecurityRequirement
            {
                {
                    new Microsoft.OpenApi.Models.OpenApiSecurityScheme
                    {
                        Reference =
                            new Microsoft.OpenApi.Models.OpenApiReference
                            {
                                Type =
                                    Microsoft.OpenApi.Models.ReferenceType.SecurityScheme,

                                Id = "Bearer"
                            }
                    },

                    Array.Empty<string>()
                }
            }
        );
    }
);


// BUILD

var app = builder.Build();


// SWAGGER

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}


// MIDDLEWARE
app.UseHttpsRedirection();

app.UseCors("Frontend");


// Authentication → Authorization sırası önemli
app.UseAuthentication();

app.UseAuthorization();

// CONTROLLERS

app.MapControllers();

// SIGNALR HUB
app.MapHub<TelemetryHub>(
    "/hubs/telemetry"
);

// RUN

app.Run();