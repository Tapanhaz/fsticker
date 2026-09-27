// Copyright (c) 2026 Tapanhaz
// Part of fsticker :: https://github.com/Tapanhaz/fsticker
// Licensed under the Apache License, Version 2.0
// Created: 2026-09-25 10:41 IST

#pragma once

#include <boost/asio/ssl/context.hpp>
#include <boost/system/error_code.hpp>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <wincrypt.h>
#include <windows.h>
#ifdef X509_NAME
#undef X509_NAME
#endif
#ifdef X509_EXTENSIONS
#undef X509_EXTENSIONS
#endif
#ifdef PKCS7_ISSUER_AND_SERIAL
#undef PKCS7_ISSUER_AND_SERIAL
#endif
#ifdef PKCS7_SIGNER_INFO
#undef PKCS7_SIGNER_INFO
#endif
#ifdef OCSP_REQUEST
#undef OCSP_REQUEST
#endif
#ifdef OCSP_RESPONSE
#undef OCSP_RESPONSE
#endif
#ifdef _MSC_VER
#pragma comment(lib, "crypt32.lib")
#endif
#else
#include <array>
#include <cstdlib>
#include <filesystem>
#include <system_error>
#endif

namespace fsticker::detail {

#ifdef _WIN32

    inline void load_platform_roots(boost::asio::ssl::context &ctx) noexcept {
        X509_STORE *store = SSL_CTX_get_cert_store(ctx.native_handle());
        if (store == nullptr) {
            return;
        }
        HCERTSTORE handle = CertOpenSystemStoreW(0, L"ROOT");
        if (handle == nullptr) {
            return;
        }
        PCCERT_CONTEXT cert = nullptr;
        while ((cert = CertEnumCertificatesInStore(handle, cert)) != nullptr) {
            const unsigned char *der = cert->pbCertEncoded;
            X509 *x509 = d2i_X509(nullptr, &der, static_cast<long>(cert->cbCertEncoded));
            if (x509 != nullptr) {
                X509_STORE_add_cert(store, x509);
                X509_free(x509);
            }
        }
        CertCloseStore(handle, 0);
        ERR_clear_error();
    }

#else

    inline void load_platform_roots(boost::asio::ssl::context &ctx) noexcept {
        if (std::getenv("SSL_CERT_FILE") != nullptr || std::getenv("SSL_CERT_DIR") != nullptr) {
            return;
        }
        static constexpr std::array<const char *, 6> candidates {
            "/etc/ssl/certs/ca-certificates.crt",
            "/etc/pki/tls/certs/ca-bundle.crt",
            "/etc/ssl/ca-bundle.pem",
            "/etc/pki/tls/cacert.pem",
            "/etc/ssl/cert.pem",
            "/usr/local/share/certs/ca-root-nss.crt"};
        for (const char *path : candidates) {
            std::error_code fs_ec;
            if (!std::filesystem::is_regular_file(path, fs_ec)) {
                continue;
            }
            boost::system::error_code ec;
            ctx.load_verify_file(path, ec);
            if (!ec) {
                return;
            }
        }
    }

#endif

    inline void configure_trust_store(boost::asio::ssl::context &ctx) noexcept {
        boost::system::error_code ec;
        ctx.set_default_verify_paths(ec);
        load_platform_roots(ctx);
    }

} // namespace fsticker::detail