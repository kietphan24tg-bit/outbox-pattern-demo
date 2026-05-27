// @ts-check
import eslint from '@eslint/js';
import globals from 'globals';
import tseslint from 'typescript-eslint';

export default tseslint.config(
    {
        ignores: ['eslint.config.mjs', 'dist/**', 'node_modules/**']
    },
    eslint.configs.recommended,
    ...tseslint.configs.recommendedTypeChecked,
    {
        languageOptions: {
            globals: {
                ...globals.node,
                ...globals.jest
            },
            sourceType: 'commonjs',
            parserOptions: {
                projectService: true,
                tsconfigRootDir: import.meta.dirname
            }
        }
    },
    {
        rules: {
            // ===== TypeScript thực tế =====
            '@typescript-eslint/no-explicit-any': 'off',
            '@typescript-eslint/no-floating-promises': 'warn',
            '@typescript-eslint/no-unsafe-argument': 'warn',
            '@typescript-eslint/no-unused-vars': 'warn',

            // ===== Tắt rule gây khó chịu =====
            '@typescript-eslint/require-await': 'off',
            '@typescript-eslint/strict-boolean-expressions': 'off',

            // ===== Tắt Prettier check trong ESLint =====
            'prettier/prettier': 'off'
        }
    }
);
