#!/usr/bin/env groovy

/**
 * Backend Pipeline Template - GitOps Deployment
 * 
 * Flow:
 *   - MR: Validate (lint, test, scan)
 *   - release/dev: Build → Deploy to Dev (auto-merge)
 *   - main: Build → Deploy to Staging + update Prod values (manual merge)
 * 
 * Usage:
 *   @Library('pipeline-shared-library') _
 *   jobseekerPipeline(service: 'api', tenant: 'shared')
 * 
 * Documentation:
 *   - Setup: docs/JENKINS-MULTIBRANCH-SETUP.md
 *   - Monorepo: docs/MONOREPO-ORCHESTRATOR-FREESTYLE.md
 */

@Library('pipeline-shared-library') _

jobseekerPipeline(
    // ═══════════════════════════════════════════════════════════════
    // REQUIRED PARAMETERS
    // ═══════════════════════════════════════════════════════════════
    service: 'worker-exporter',                          // Service name (e.g., 'core-ats', 'auth-service')
    tenant: 'jobseeker-platform',            // Tenant namespace (e.g., 'jobseeker-platform', 'fithub')
    
    // ═══════════════════════════════════════════════════════════════
    // AGENTS (dedicated Jenkins agents)
    // ═══════════════════════════════════════════════════════════════
    buildAgent: 'agent-for-building',
    testAgent: 'agent-for-testing',
    gitopsAgent: 'agent-for-gitops',
    
    // ═════════════════════════════════════```23══════════════════════════
    // BUILD CONFIGURATION
    // ═══════════════════════════════════════════════════════════════
    buildStrategy: 'auto',                   // auto|springboot|nestjs|express|nodejs|react|vue|laravel|lumen|ci3|rust|docker

    // ECR Registry group (for jobseeker-platform services only)
    // platformRepoGroup: 'core',             // core|mobile|worker (final path: jobseeker-platform/<group>/<service>)
    
    // ═══════════════════════════════════════════════════════════════
    // MONOREPO CONFIGURATION (optional)
    // ═══════════════════════════════════════════════════════════════
    // isMonorepo: true,
    // monorepoPath: 'apps/api',              // Path to service within monorepo
    // monorepoSharedPaths: ['packages', 'Dockerfile', 'pnpm-lock.yaml'],  // Changes here validate ALL services
    // monorepoRootDockerfilePath: 'Dockerfile', // Optional: override root Dockerfile location
    // monorepoRootFiles: ['Dockerfile', 'pnpm-lock.yaml'], // Files that trigger all services
    
    // ═══════════════════════════════════════════════════════════════
    // QUALITY GATES (MR Validation Stage)
    // ═══════════════════════════════════════════════════════════════
    enableLint: true,                        // Code linting (eslint, ktlint, etc.)
    enableUnitTest: true,                    // Unit tests with coverage
    enableCoverageCheck: true,
    coverageThreshold: 60.0,                 // Fail if coverage < 60%
    enableSecretScan: true,                  // Gitleaks secret detection
    enableDependencyScan: true,              // Trivy FS - dependency & config scanning
    enablePreCompile: false,                 // Pre-compile check (Java only, MR validation)
    
    // ═══════════════════════════════════════════════════════════════
    // QUALITY GATES (Build Stage)
    // ═══════════════════════════════════════════════════════════════
    enableIntegrationTest: true,             // Integration tests
    enableImageScan: true,                   // Trivy image - container vulnerability scan
    enableSonarQube: true,                   // SonarQube code analysis
    enableBuildUnitTest: true,               // Re-run tests for SonarQube coverage in build
    enableQualityGate: true,                 // Enforce SonarQube quality gate
    sonarQubeServer: 'SonarQube',            // SonarQube server instance name
    
    // ═══════════════════════════════════════════════════════════════
    // RUNTIME VALIDATION
    // ═══════════════════════════════════════════════════════════════
    enableHealthCheck: false,                 // Post-deploy health check
    healthEndpoint: '/healthz',      // Spring Boot: /actuator/health, Node.js: /health
    healthCheckRetries: 5,
    healthCheckInterval: 10,
    
    // ═══════════════════════════════════════════════════════════════
    // FAILURE BEHAVIOR
    // ═══════════════════════════════════════════════════════════════
    gatekeeper: false,                       // true = fail on quality gate issues, false = warnings only
    failOnCritical: true,                    // Fail on CRITICAL vulnerabilities
    failOnSecrets: true,                     // Fail on detected secrets
    
    // ═══════════════════════════════════════════════════════════════
    // BUILD METADATA (optional)
    // ═══════════════════════════════════════════════════════════════
    // generateVersionFile: true,
    // versionFile: 'src/main/resources/version.properties',  // Java
    // versionFile: 'version.json',                           // Node.js
    
    // ═══════════════════════════════════════════════════════════════
    // LOGGING
    // ═══════════════════════════════════════════════════════════════
    enableAnsiColors: true                   // Colored console output
)
