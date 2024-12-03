# gocacheprog-s3

`gocacheprog-s3` is a "cacheprog" for Go that uses S3.

⚠️ Apparently a few people have actually found this repo and may even be using it! ⚠️ Consider this a proof-of-concept/prototype! :warning: I wrote it in a fugue state. ⚠️ Do not use for real. ⚠️ Do not expect support. ⚠️ Do not expect PRs to be looked at, reviewed, or merged. ⚠️ Do not water after midnight. ⚠️

(That said, if you do find it's useful and want to take over and do real releases, do feel free to reach out.)

# Usage

[As of writing (Apr 2024), `GOCACHEPROG` support requires a custom build of the Go toolchain](https://github.com/golang/go/issues/64876). As such, we provide a Docker image with both a custom build of the Go toolchain and the `gocacheprog-s3` binary baked in.

```
ghcr.io/nfi-hashicorp/gocacheprog-s3:latest-go1.22.2
```

## Docker example

```console
% docker run --platform=linux/amd64 -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN -e AWS_REGION -e GOCACHEPROG="gocacheprog-s3 -v=2 mycoolbucket" ghcr.io/nfi-hashicorp/gocacheprog-s3:go1.22.2-latest go install github.com/nfi-hashicorp/gocacheprog-s3@latest
...
disk stats: 
413 gets: 0 hits, 413 misses, 0 errors, 0s total dur
901 puts: 0 errors, 0s total dur
s3 stats: 
414 gets: 1 hits, 413 misses, 0 errors, 0s total dur; total 0.00 MB; avg 0.00 MB/s
902 puts: 0 errors, 1m17.3s total dur; total 127.59 MB; avg 1.65 MB/s
```

And then again to see the speed up.

```console
% docker run --platform=linux/amd64 -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN -e AWS_REGION -e GOCACHEPROG="gocacheprog-s3 -v=2 mycoolbucket" ghcr.io/nfi-hashicorp/gocacheprog-s3:go1.22.2-latest go install github.com/nfi-hashicorp/gocacheprog-s3@latest
...
disk stats: 
642 gets: 0 hits, 642 misses, 0 errors, 0s total dur
642 puts: 0 errors, 0s total dur
s3 stats: 
643 gets: 642 hits, 1 misses, 0 errors, 34.8s total dur; total 127.53 MB; avg 3.66 MB/s
2 puts: 0 errors, 300ms total dur; total 0.00 MB; avg 0.00 MB/s
```

## Local dev example

```console
% GOCACHEPROG="./gocacheprog-s3 -v=2 -local-cache-dir=go-cache $BUCKET"
% aws s3 rm --recursive s3://$BUCKET/go-cache > /dev/null 2>&1
% rm -rf go-cache
% go build .
disk stats: 
413 gets: 0 hits, 413 misses, 0 errors, 0s total dur
893 puts: 0 errors, 0s total dur
s3 stats: 
414 gets: 1 hits, 413 misses, 0 errors, 0s total dur; total 0.00 MB; avg 0.00 MB/s
894 puts: 0 errors, 1m18.7s total dur; total 140.99 MB; avg 1.79 MB/s
total time:  1m21s
```

And then it goes *much* faster the second time.

```console
% rm -rf go-cache
% go build .     
disk stats: 
641 gets: 0 hits, 641 misses, 0 errors, 0s total dur
641 puts: 0 errors, 0s total dur
s3 stats: 
642 gets: 641 hits, 1 misses, 0 errors, 35.8s total dur; total 140.42 MB; avg 3.92 MB/s
2 puts: 0 errors, 300ms total dur; total 0.00 MB; avg 0.01 MB/s
total time:  12s
```

# Credits

Derived from https://github.com/bradfitz/go-tool-cache and [or-shachar/go-tool-cache](https://github.com/or-shachar/go-tool-cache/commit/cc47faab56325a022ff59cd7277abbf99ff4f8ff).
