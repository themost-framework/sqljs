const { pathsToModuleNameMapper } = require('ts-jest');
const { compilerOptions }  = require('./tsconfig.json');
/** @type {import('ts-jest').JestConfigWithTsJest} **/
module.exports = {
  testEnvironment: "jest-environment-jsdom",
  roots: ['<rootDir>'],
  modulePaths: [compilerOptions.baseUrl],
    moduleNameMapper: pathsToModuleNameMapper(compilerOptions.paths, { prefix: '<rootDir>' }),
  transform: {
    "^.+.tsx?$": ["ts-jest",{
        tsconfig: "tsconfig.json"
    }]
  }
}
