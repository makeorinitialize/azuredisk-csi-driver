// Copyright 2026 The Kubernetes Authors.
// Licensed under the Apache License, Version 2.0.

package deviceuse

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"net"
	"net/url"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"

	pb "sigs.k8s.io/azuredisk-csi-driver/pkg/deviceuse/usev1"
)

func testCertificates(t *testing.T) (tls.Certificate, *x509.CertPool, func(string) tls.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), IsCA: true, BasicConstraintsValid: true,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageCertSign}
	raw, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(raw)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	serial := int64(2)
	issue := func(uri string) tls.Certificate {
		t.Helper()
		private, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		serial++
		cert := &x509.Certificate{SerialNumber: big.NewInt(serial), BasicConstraintsValid: true,
			NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature}
		if uri == "" {
			cert.DNSNames, cert.ExtKeyUsage = []string{"provider"}, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		} else {
			u, err := url.Parse(uri)
			if err != nil {
				t.Fatal(err)
			}
			cert.URIs, cert.ExtKeyUsage = []*url.URL{u}, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
		}
		raw, err := x509.CreateCertificate(rand.Reader, cert, ca, &private.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		return tls.Certificate{Certificate: [][]byte{raw}, PrivateKey: private}
	}
	return issue(""), roots, issue
}

func TestDeviceUseMutualTLSNodeAndSourceScopeOnlyThreeRPCs(t *testing.T) {
	d, _, _, id := fixture(t)
	cert, roots, issue := testCertificates(t)
	server, err := NewServer(d, cert, roots, []Grant{
		{URI: "spiffe://test/node", Node: "node", Namespace: "sources", PVNames: []string{"pv"}},
		{URI: "spiffe://test/reader", Node: "node", Namespace: "sources", PVNames: []string{"pv"}, ReadOnly: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	connect := func(uri string) *grpc.ClientConn {
		t.Helper()
		conf := &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, ServerName: "provider"}
		if uri != "" {
			conf.Certificates = []tls.Certificate{issue(uri)}
		}
		conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(conf)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return conn
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	conn := connect("spiffe://test/node")
	client := pb.NewDeviceUseClient(conn)
	got, err := client.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: id})
	requireCode(t, err, codes.OK)
	if got.State != pb.UseState_USE_STATE_ACTIVE || !proto.Equal(got.Identity, id) {
		t.Fatal(got)
	}
	foreign := pb.NewDeviceUseClient(connect("spiffe://test/foreign"))
	_, err = foreign.ReleaseUse(ctx, &pb.ReleaseUseRequest{Identity: id})
	requireCode(t, err, codes.PermissionDenied)
	reader := pb.NewDeviceUseClient(connect("spiffe://test/reader"))
	got, err = reader.GetUse(ctx, &pb.GetUseRequest{Identity: id})
	requireCode(t, err, codes.OK)
	if got.State != pb.UseState_USE_STATE_ACTIVE {
		t.Fatal(got)
	}
	_, err = reader.ReleaseUse(ctx, &pb.ReleaseUseRequest{Identity: id})
	requireCode(t, err, codes.PermissionDenied)
	_, err = reader.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: id})
	requireCode(t, err, codes.PermissionDenied)
	wrong := proto.CloneOf(id)
	wrong.UseId, wrong.Source.PvName = "other", "other-pv"
	_, err = client.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: wrong})
	requireCode(t, err, codes.PermissionDenied)
	wrong = proto.CloneOf(id)
	wrong.NodeId = "other-node"
	_, err = client.ReleaseUse(ctx, &pb.ReleaseUseRequest{Identity: wrong})
	requireCode(t, err, codes.InvalidArgument)
	err = conn.Invoke(ctx, "/compositedisk.provider.v1.DeviceProvider/Reserve", &pb.AcquireUseRequest{Identity: id}, new(pb.Use))
	requireCode(t, err, codes.Unimplemented)
	unverified := pb.NewDeviceUseClient(connect(""))
	if _, err := unverified.GetUse(ctx, &pb.GetUseRequest{Identity: id}); err == nil {
		t.Fatal("unauthenticated client reached the use service")
	}
	got, err = client.ReleaseUse(ctx, &pb.ReleaseUseRequest{Identity: id})
	requireCode(t, err, codes.OK)
	if got.State != pb.UseState_USE_STATE_RELEASED {
		t.Fatal(got)
	}
}
