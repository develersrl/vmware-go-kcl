host$ docker-compose run --rm --publish 40000:40000 kcl bash

docker$ go get github.com/derekparker/delve/cmd/dlv
docker$ dlv test --listen=:40000 --headless=true --api-version=2 --log --check-go-version=false --build-flags '--tags=integration'

VsCode:
        {
            "name": "Delve into Docker",
            "type": "go",
            "request": "attach",
            "mode": "remote",
            "remotePath": "/go/src/github.com/vmware/vmware-go-kcl",
            "port": 40000,
            "host": "171.26.0.1",
            "showLog": true
      }

Place a breakpoint in the test and run that configuration!

Enjoy!
