# 🐳 Docker Setup for Logcomex Importer

This Docker setup provides a complete containerized environment for the Logcomex Importer API with MySQL database, performance testing, and optional Nginx reverse proxy.

## 🚀 Quick Start

### Prerequisites
- Docker Desktop (Windows/Mac) or Docker Engine (Linux)
- Docker Compose
- At least 4GB RAM available for Docker

### 1. Start Development Environment (Recommended)
```bash
# PowerShell
.\docker-setup.ps1 -Environment dev -Action start

# Or using Makefile
make dev
```

### 2. Start Production Environment
```bash
# PowerShell
.\docker-setup.ps1 -Environment prod -Action start

# Or using Makefile
make start
```

### 3. Test the Setup
```bash
# PowerShell
.\docker-setup.ps1 -Action test

# Or using Makefile
make test
```

## 🌐 Access URLs

| Service | URL | Description |
|---------|-----|-------------|
| **API** | http://localhost:8000 | Main API endpoint |
| **API Docs** | http://localhost:8000/docs | Interactive API documentation |
| **Health Check** | http://localhost:8000/health | API health status |
| **Ping Test** | http://localhost:8000/ping | Fast response test |
| **Database Admin** | http://localhost:8080 | Adminer (dev only) |

## 📋 Available Commands

### PowerShell Script
```powershell
# Start services
.\docker-setup.ps1 -Environment dev -Action start
.\docker-setup.ps1 -Environment prod -Action start

# Stop services
.\docker-setup.ps1 -Action stop

# Restart services
.\docker-setup.ps1 -Action restart

# View logs
.\docker-setup.ps1 -Action logs

# Run performance tests
.\docker-setup.ps1 -Action test

# Clean environment
.\docker-setup.ps1 -Action clean

# Build services
.\docker-setup.ps1 -Action build
```

### Makefile (Cross-platform)
```bash
# Development environment
make dev

# Production environment
make start

# Stop all services
make stop

# Restart services
make restart

# View logs
make logs
make logs-dev

# Run tests
make test
make test-dev

# Clean environment
make clean

# Build services
make build

# Show status
make status

# Quick endpoint tests
make ping
make health
make status-api
```

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Nginx (80)   │    │  Performance    │    │   Adminer      │
│  (Optional)    │    │    Tester       │    │    (8080)      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │   FastAPI       │
                    │   (8000)        │
                    └─────────────────┘
                                 │
                    ┌─────────────────┐
                    │   MySQL (3306)  │
                    └─────────────────┘
```

## 🔧 Configuration

### Environment Variables
The following environment variables are automatically set in the containers:

| Variable | Value | Description |
|----------|-------|-------------|
| `DB_HOST` | `mysql` | Database hostname |
| `DB_PORT` | `3306` | Database port |
| `DB_NAME` | `logcomex` | Database name |
| `DB_USER` | `sarvesh` | Database username |
| `DB_PASSWORD` | `Saved6-Hydrogen-Smirk-Paltry-Trimmer` | Database password |
| `API_KEY` | `n8HPUtVG16Ea5mYi5jvlOYqFyObzTd1ZvXMTjU8s` | Logcomex API key |

### Custom Configuration
To use custom configuration:

1. **Create `.env` file:**
```bash
DB_HOST=your-mysql-host
DB_PORT=3306
DB_NAME=your-database
DB_USER=your-username
DB_PASSWORD=your-password
API_KEY=your-api-key
```

2. **Update docker-compose files** to use environment file:
```yaml
environment:
  - DB_HOST=${DB_HOST}
  - DB_PASSWORD=${DB_PASSWORD}
  # ... etc
```

## 📊 Performance Testing

### Automated Testing
```bash
# Run full performance test suite
make test

# Or using PowerShell
.\docker-setup.ps1 -Action test
```

### Manual Testing
```bash
# Test individual endpoints
make ping        # Should respond in <50ms
make health      # Should respond in <100ms
make status-api  # Should respond in <1s

# Test with curl
curl http://localhost:8000/ping
curl http://localhost:8000/health
curl http://localhost:8000/status
```

### Expected Performance
- **`/ping`**: < 50ms response time
- **`/health`**: < 100ms response time
- **`/status`**: < 1 second response time
- **Concurrent requests**: 15-25 requests/second

## 🗄️ Database Management

### Access Database
```bash
# Open MySQL shell
make db-shell

# Or directly
docker-compose exec mysql mysql -u sarvesh -pSaved6-Hydrogen-Smirk-Paltry-Trimmer logcomex
```

### Database Admin Interface
```bash
# Open Adminer (development only)
make db-admin

# Then open http://localhost:8080 in browser
# Server: mysql
# Username: sarvesh
# Password: Saved6-Hydrogen-Smirk-Paltry-Trimmer
# Database: logcomex
```

### Export Data
```bash
# Export records to CSV
make export-records

# Export summaries to CSV
make export-summaries
```

## 🚨 Troubleshooting

### Common Issues

#### 1. Port Already in Use
```bash
# Check what's using the port
netstat -ano | findstr :8000

# Kill the process or change ports in docker-compose files
```

#### 2. Database Connection Issues
```bash
# Check database status
docker-compose logs mysql

# Restart database
docker-compose restart mysql
```

#### 3. API Not Starting
```bash
# Check API logs
docker-compose logs api

# Check if database is ready
docker-compose exec mysql mysqladmin ping -h localhost
```

#### 4. Performance Still Slow
```bash
# Ensure async optimizations are working
curl http://localhost:8000/ping  # Should be very fast

# Check database indexes
docker-compose exec mysql mysql -u sarvesh -pSaved6-Hydrogen-Smirk-Paltry-Trimmer logcomex -e "SHOW INDEX FROM import_records;"
```

### Debug Mode
```bash
# Start with debug logging
docker-compose -f docker-compose.dev.yml up -d

# View real-time logs
docker-compose -f docker-compose.dev.yml logs -f api
```

## 🔄 Development Workflow

### 1. Start Development Environment
```bash
make dev
```

### 2. Make Code Changes
- Edit `main.py` or other files
- Changes automatically reload due to `--reload` flag

### 3. Test Changes
```bash
# Test individual endpoints
make ping
make health

# Run full performance test
make test-dev
```

### 4. View Logs
```bash
make logs-dev
```

### 5. Stop Services
```bash
make stop
```

## 🚀 Production Deployment

### 1. Build Production Image
```bash
make build
```

### 2. Start Production Services
```bash
make start
```

### 3. Monitor Services
```bash
make status
make logs
```

### 4. Scale Services (Optional)
```bash
# Scale API to multiple instances
docker-compose up -d --scale api=3
```

## 📁 File Structure

```
logcomex-importer/
├── Dockerfile                 # Main application container
├── docker-compose.yml         # Production services
├── docker-compose.dev.yml     # Development services
├── docker-setup.ps1          # PowerShell management script
├── Makefile                   # Cross-platform management
├── .dockerignore             # Docker build exclusions
├── requirements.txt           # Python dependencies
├── main.py                   # FastAPI application
├── performance_test.py        # Performance testing
├── exports/                  # CSV export directory
└── logs/                     # Application logs
```

## 🔒 Security Notes

- Database passwords are hardcoded in compose files for development
- For production, use environment variables or Docker secrets
- Consider using Docker secrets for sensitive data
- Database is exposed on localhost:3306 - restrict access in production

## 📈 Monitoring & Metrics

### Health Checks
All services include health checks:
- **API**: HTTP endpoint `/health`
- **MySQL**: `mysqladmin ping`
- **Nginx**: HTTP endpoint check

### Logs
```bash
# View all logs
make logs

# View specific service logs
docker-compose logs api
docker-compose logs mysql
```

### Resource Usage
```bash
# Check container resource usage
docker stats

# Check disk usage
docker system df
```

## 🎯 Next Steps

1. **Start with development environment**: `make dev`
2. **Test basic endpoints**: `make ping`, `make health`
3. **Run performance tests**: `make test-dev`
4. **Explore API documentation**: http://localhost:8000/docs
5. **Customize configuration** as needed
6. **Deploy to production** when ready

## 🆘 Support

If you encounter issues:

1. Check the troubleshooting section above
2. View service logs: `make logs`
3. Check service status: `make status`
4. Restart services: `make restart`
5. Clean and rebuild: `make clean && make build`

---

**Happy containerizing! 🐳✨**
